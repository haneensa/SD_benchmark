import numpy as np
import pandas as pd
import duckdb
import json
from pygg import *

# Source Sans Pro Light
legend = theme_bw() + theme(**{
  "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
  "legend.justification":"c(1,0)", "legend.position":"c(1,0)",
  "legend.key" : element_blank(),
  "legend.title":element_blank(),
  "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
  "axis.text": element_text(colour = "'#333333'", size=11),  
  "plot.background": element_blank(),
  "panel.border": element_rect(color=esc("#e0e0e0")),
  "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
  "strip.text": element_text(color=esc("#333333"))
  
})
# need to add the following to ggsave call:
#    libs=['grid']
legend_bottom = legend + theme(**{
  "legend.position":esc("bottom"),
  #"legend.spacing": "unit(-.5, 'cm')"

})

legend_side = legend + theme(**{
  "legend.position":esc("right"),
})

def getOperatorRuntime(plan, op, sys):
    """
    given a query plan and an operator from duckdb,
    return the operator's runtime in the query plan
    """
    plan= plan.replace("'", "\"")
    plan = json.loads(plan)
    if op == "scan" or op == "filter_scan":
        op ="SEQ_SCAN1"
        if sys == "SD_Capture":
            op ="SEQ_SCAN_01"
    elif op == "orderby":
        op = "ORDER_BY1"
        if sys == "SD_Capture":
            op = "ORDER_BY_11"
    elif op == "filter":
        op ="FILTER1"
        if sys == "SD_Capture":
            op ="FILTER_11"
    elif op == "reg_agg":
        op ="HASH_GROUP_BY1"
        if sys == "SD_Capture":
            op ="HASH_GROUP_BY_21"
        if op not in plan:
            op ="HASH_GROUP_BY3"
    elif op == "perfect_agg":
        op ="PERFECT_HASH_GROUP_BY1"
        if sys == "SD_Capture":
            op ="PERFECT_HASH_GROUP_BY_21"
        if op not in plan:
            op ="PERFECT_HASH_GROUP_BY3"
    elif op == "merge":
        op ="PIECEWISE_MERGE_JOIN2"
        if sys == "SD_Capture":
            op ="PIECEWISE_MERGE_JOIN_21"
            if op not in plan:
                op ="PIECEWISE_MERGE_JOIN_22"
        if op not in plan:
            op ="PIECEWISE_MERGE_JOIN1"
    elif op == "nl":
        op ="NESTED_LOOP_JOIN2"
        if sys == "SD_Capture":
            op ="NESTED_LOOP_JOIN_22"
            if op not in plan:
                op ="NESTED_LOOP_JOIN_21"
        if op not in plan:
            op ="NESTED_LOOP_JOIN1"
    elif op == "bnl":
        op ="BLOCKWISE_NL_JOIN1"
        if sys == "SD_Capture":
            op ="BLOCKWISE_NL_JOIN_21"
        if op not in plan:
            op ="BLOCKWISE_NL_JOIN2"
    elif op == "cross":
        op ="CROSS_PRODUCT1"
        if op not in plan:
            op ="CROSS_PRODUCT2"
        if sys == "SD_Capture":
            op ="CROSS_PRODUCT_22"
            if op not in plan:
                op ="CROSS_PRODUCT_21"
    elif op == "index_join_pkfk":
        op ="INDEX_JOIN1"
        if op not in plan:
            op ="INDEX_JOIN2"
        if sys == "SD_Capture":
            op ="INDEX_JOIN_21"
            if op not in plan:
                op ="INDEX_JOIN_22"
    elif op == "hash_join_pkfk":
        op ="HASH_JOIN1"
        if op not in plan:
            op ="HASH_JOIN2"
        if sys == "SD_Capture":
            op ="HASH_JOIN_22"
            if op not in plan:
                op ="INDEX_JOIN_22"
    if op in plan:
        return plan[op]
    else:
        print("ERROR", op, plan)
        return 1

def getAllExec(plan, op, sys):
    """
    return execution time (sec) from profiling
    data stored in query plan
    """
    plan = plan.replace("'", "\"")
    plan = json.loads(plan)
    total = 0.0
    for k, v in plan.items():
        total += float(v)
    return total

def getMat(plan, op, sys):
    """
    return materialization time (sec) from
    profiling data stored in query plan
    """
    plan= plan.replace("'", "\"")
    plan = json.loads(plan)
    if sys == "SD_Capture":
        key = "CREATE_TABLE_AS_10"
        if key not in plan:
            key = "CREATE_TABLE_AS_20"
        if key not in plan:
            key = "CREATE_TABLE_AS_30"
        if key not in plan:
            key = "CREATE_TABLE_AS_40"
        return plan[key]
    else:
        return plan["CREATE_TABLE_AS0"]

def fanout(a, b):
    if b == 0: return 0
    return a / b
    
def get_n2(row):
    if row["query"] in ["cross", "nl", "bnl", "merge", "index_join_pkfk", "hash_join_pkfk"]:
        return row["groups"].split(",")[0]
    else:
        return 0

def get_skew(row):
    if row["query"] in ["index_join_pkfk", "hash_join_pkfk"]:
        return row["groups"].split(",")[2]
    else:
        return 0

def get_p(row):
    if row["query"] in ["filter", "filter_scan", "scan", "orderby"]:
        return row["p"]+3
    else:
        return row["p"]

def get_groups(row):
    if row["query"] in ["filter", "filter_scan", "merge", "nl", "bnl", "cross"]:
        return -1
    elif row["query"] in ["index_join_pkfk", "hash_join_pkfk"]:
        return row["groups"].split(",")[0]
    else:
        return row["groups"]

def get_sel(row):
    if row["query"] in ["cross", "nl", "bnl", "merge", "index_join_pkfk", "hash_join_pkfk"]:
        return round(1.0 - float(row["groups"].split(",")[1]), 1)
    elif row["query"] in ["filter", "filter_scan"]:
        return row["groups"]
    else:
        return 1.0

def get_index_join_qtype(row):
    if row["query"] in ["index_join_pkfk"]:
        index_scan =  row["groups"].split(",")[3]
        if index_scan == "False":
            return "Q_P,F"
        else:
            return "Q_P"
    else:
        return "-"

lcopy = "a12_copy"
lfull = "a12_full"
lstats = "a12_stats"
lday = "a12"
def get_lineage_type(row):
    if row["lineage_type"] != "SD_Capture":
        return row["lineage_type"]

    if row["notes"] == lcopy:
        return "SD_Copy"
    elif row["notes"] == lfull:
        return "SD_Full"
    elif row["notes"] == lstats:
        return "SD_Stats"
    else:
        return "unknown"

df_all = pd.read_csv("eval_results/micro_benchmark_notes_a12.csv")
df_all["card2"] = 0
df_all["sel"] = 0
df_logical = df_all[df_all["notes"]==lday]
df_logical["notes"] = "logical"
df_stats = df_all[df_all["notes"]==lstats]#pd.read_csv("eval_results/micro_benchmark_notes_feb26b_stats.csv")
df_full = df_all[df_all["notes"]==lfull]
df_copy = df_all[df_all["notes"]==lcopy]

df_data = df_logical
df_data = df_data.append(df_full)
df_data = df_data.append(df_copy)
df_data["op_runtime"] = df_data.apply(lambda x: getOperatorRuntime(x['plan_timings'], x['query'], x["lineage_type"]), axis=1)
df_data["mat_time"] = df_data.apply(lambda x: getMat(x['plan_timings'], x['query'], x["lineage_type"]), axis=1)
df_data["plan_runtime"] = df_data.apply(lambda x: getAllExec(x['plan_timings'], x['query'], x["lineage_type"]), axis=1)

df_stats["op_runtime"] = -1
df_stats["mat_time"] = -1
df_stats["plan_runtime"] = -1

for df in [df_data, df_stats]:
    df["n1"] = df["cardinality"]
    df["n2"] = df.apply(lambda x : get_n2(x), axis=1)
    df["sel"] = df.apply(lambda x : get_sel(x), axis=1)
    df["skew"] = df.apply(lambda x : get_skew(x), axis=1)
    df["p"] = df.apply(lambda x : get_p(x), axis=1)
    df["index_join"] = df.apply(lambda x : get_index_join_qtype(x), axis=1)
    df["groups"] = df.apply(lambda x : get_groups(x), axis=1)
    df["lineage_type"] = df.apply(lambda x :get_lineage_type(x) , axis=1)

df_final = df_data
df_final = df_final.append(df_stats)


print(list(df_final.columns))
metrics = ["runtime", "output", "op_runtime", "mat_time", "plan_runtime"]
header_unique = ["query", "n1", "n2", "skew", "p", "sel", "groups", "r", "index_join", "lineage_type"]

header = header_unique + metrics
processed_data = pd.DataFrame(df_final[header]).reset_index(drop=True)
file_name = "micro.csv"
processed_data.to_csv(file_name, encoding='utf-8',index=False)


#print(processed_data)

# schema: query, lineage_type, n_cols, runtime_s, 
#         n1, n2, output, groups, a, size_MB, 
#         postprocess_ms, baseExec, materialization,
#         execution
con = duckdb.connect()

header_unique = ["query", "n1", "n2", "skew", "p", "sel", "groups",  "index_join", "lineage_type"]
g = ','.join(header_unique)
print(g)
con.execute("CREATE TABLE micro_test AS SELECT * FROM '{}';".format(file_name))
con.execute("COPY (SELECT * FROM micro_test) TO '{}' WITH (HEADER 1, DELIMITER '|');".format(file_name))
print(con.execute("pragma table_info('micro_test')").fetchdf())

# average over the different runs (r)
print(con.execute("""create table avg_micro as
                    select {}, avg(plan_runtime) as plan_runtime, avg(runtime) as runtime,
                            avg(output) as output, avg(op_runtime) as op_runtime, avg(mat_time) as mat_time from micro_test
                            group by {}""".format(g, g)).fetchdf())
header_unique.remove("lineage_type")
g = ','.join(header_unique)
m = ','.join(metrics)
#con.execute("pragma enable_profiling")
print(con.execute("""create table micro_withBaseline as select t1.plan_runtime as base_plan_runtime, t1.runtime as base_runtime,
                  t1.output as base_output, t1.op_runtime as base_op_runtime, t1.mat_time as base_mat_time,
                  t2.* from (select {}, {} from avg_micro where lineage_type='Baseline') as t1 join avg_micro as t2 using ({}) 
                  """.format(g, m, g)).fetchdf())

"""
Relative overhead: percentage increase in execution time  caused by the operation  compared to some baseline.
sys (uint) execution of system of interest
baseSys (uint) baseline execution
base (uint) runtime of baseline execution

(sys-baseSys) -> overhead of system compared to baseline
h = max(((sys-baseSys)/base)*100, 0)
"""

# calculate metrics for Perm
print(con.execute("""create table micro_perm_metrics as select {},'Logical' as lineage_type,
                            (runtime - base_runtime)*1000 as overhead,
                            ((runtime - base_runtime)/base_op_runtime)*100 as rel_overhead,
                            ((plan_runtime - mat_time) - (base_plan_runtime - base_mat_time))*1000 as exec_overhead,
                            (((plan_runtime - mat_time) - (base_plan_runtime - base_mat_time))/base_op_runtime)*100 as exec_rel_overhead,
                            (mat_time - base_mat_time)*1000 as mat_overhead,
                            ((mat_time - base_mat_time) / base_op_runtime) *100 as mat_rel_overhead,
                            output / base_output as fanout
                  from micro_withBaseline where lineage_type='Perm' """.format(g)).fetchdf())

# calculate metrics for SmokedDuck
con.execute("""create table micro_sd_metrics as select {}, 'SD' as lineage_type,
                            (sd_full.op_runtime - sd_full.base_op_runtime)*1000 as overhead,
                            ((sd_full.op_runtime - sd_full.base_op_runtime)/sd_full.base_op_runtime) * 100 as rel_overhead,
                            ((sd_copy.op_runtime-sd_copy.base_op_runtime))*1000 as exec_overhead,
                            ((sd_copy.op_runtime-sd_copy.base_op_runtime)/sd_full.base_op_runtime)*100 as exec_rel_overhead,
                            ((sd_full.op_runtime - sd_full.base_op_runtime)-(sd_copy.op_runtime-sd_copy.base_op_runtime))*1000 as mat_overhead,
                            (((sd_full.op_runtime - sd_full.base_op_runtime)-(sd_copy.op_runtime-sd_copy.base_op_runtime))/sd_full.base_op_runtime)*100 as mat_rel_overhead,
                            sd_full.output / sd_full.base_output as fanout
                     from (select * from micro_withBaseline where lineage_type='SD_Copy') as sd_copy JOIN
                          (select * from micro_withBaseline where lineage_type='SD_Full') as sd_full
                          USING ({})
                  """.format(g, g)).fetchdf()
df = con.execute("""select * from micro_sd_metrics union all select * from micro_perm_metrics""").fetchdf()
data = []
for index, row in df.iterrows():
    data.append(dict(overheadType="mat", n1=row['n1'], n2=row['n2'], sel=row['sel'], p=row['p'], skew=row['skew'], g=row['groups'],
                     index_join=row['index_join'], system=row['lineage_type'], op_type=row['query'],
                     overhead=max(row['mat_overhead'], 0), roverhead=max(row['mat_rel_overhead'], 0)))
    data.append(dict(overheadType="exec", n1=row['n1'], n2=row['n2'], sel=row['sel'], p=row['p'], skew=row['skew'], g=row['groups'],
                     index_join=row['index_join'], system=row['lineage_type'], op_type=row['query'],
                     overhead=max(row['exec_overhead'], 0), roverhead=max(row['exec_rel_overhead'], 0)))
    #data.append(dict(overheadType="all", n1=row['n1'], n2=row['n2'], sel=row['sel'], p=row['p'], skew=row['skew'], g=row['groups'],
    #                 index_join=row['index_join'], system=row['lineage_type'], op_type=row['query'],
    #                 overhead=max(row['overhead'], 0), roverhead=max(row['rel_overhead'], 0)))

# join with baseline to compute overhead
con.execute("DROP TABLE queries_list")

def PlotLines(op, lfunction, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, wrap=None):
    if op:
        pdata =  [d for d in data if d['op_type']==op and lfunction(d)]
    else:
        pdata =  [d for d in data if lfunction(d)]
    if linetype:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color, linetype=linetype))
        p += geom_point()
    else:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color))
    
    p +=  geom_line(stat=esc('identity'), alpha=0.8, width=0.5)
    p += axis_labels(x_label, y_label, x_type, y_type)
    if facet:
        p += facet_grid(facet, scales=esc("free_y"))
    if wrap:
        p += facet_wrap(wrap)
    p += legend_bottom
    p += legend_side
    ggsave(fname, p,  width=w, height=h, scale=0.8)
        
y_axis_list = ["roverhead", "overhead"]
y_header = ["Relative\nOverhead %", "Overhead (ms)"]
linetype = "overheadType"
true_function  = lambda x: True
def plot_scans():
    ops = ["scan", "orderby"]
    for op in ops:
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "p", "# cols", "system", "~n1"
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            x_type, y_type, y_label = "continuous",  "continueous", "{}".format(y_header[idx])
            fname, w, h = "micro_{}_line_{}.png".format(y_axis, op), 8, 3
            lambda_function  = lambda x: True
            PlotLines(op, true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_filters():
    ops = ["filter", "filter_scan"]
    for op in ops:
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~n1~p"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
            fname, w, h = "micro_{}_line_{}.png".format(y_axis, op), 8, 4

            PlotLines(op, true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

            x_axis, x_label, color, facet = "p", "Projected Columns", "system", "~n1~sel"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
            fname, w, h = "micro_{}_projection_line_{}.png".format(y_axis, op), 8, 4

            PlotLines(op, true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

            lambda_function  = lambda x: x['n1'] == 10000000 and x['p']==3
            x_axis, x_label, color, facet= "sel", "Selectivity", "system", None#"~p"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
            fname, w, h = "micro_{}_10M_p3_line_{}.png".format(y_axis, op), 5, 2.5

            PlotLines(op, lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_cross():
    #### cross
    # vary n1 of n2
    # both systems dominated by materializations overhead
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "n2", "n1", "system", None
        x_type, y_type, y_label = "continuous", "log10",  "{} [log]".format(header[idx])
        fname, w, h = "micro_{}_cross.png".format(y_axis), 5.5, 2.5
        PlotLines("cross", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_ineq_joins():
    # joins inequiality
    ## 1) X-axis: selectivity Y-axis: relative overhead | overhead; group: n1
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~op_type~n2"
        #x_type, y_type, y_label = "continuous",  x_type, "{}".format(header[idx])
        x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(header[idx])
        fname, w, h = "micro_{}_line_ineq.png".format(y_axis), 8, 4
        lambda_function  = lambda x: x['op_type'] in ["nl", "merge", "bnl"]
        PlotLines(None, lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

    for idx, y_axis in enumerate(y_axis_list):
        lambda_function  = lambda x: x['op_type'] in ["nl", "merge", "bnl" and x['n2']==1000000]
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~op_type"
        #x_type, y_type, y_label = "continuous",  x_type, "{}".format(header[idx])
        x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(header[idx])
        fname, w, h = "micro_{}_10M_1k_line_ineq.png".format(y_axis), 6, 2.5
        PlotLines(None, lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_index_join():
    ps = ["Q_P,F", "Q_P"]
    for p in ps:
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew~index_join~g~n1"
            #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
            fname, w, h = "micro_{}_line_indexJoin.png".format(y_axis), 8, 10

            PlotLines("index_join_pkfk", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
            
            lambda_function  = lambda x: x['n1'] == 10000000 and x['n2']==1000 and x['index_join']==p

            x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew~g"
            #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
            fname, w, h = "micro_{}_{}_10M_1k_line_indexJoin.png".format(y_axis, p), 6, 3

            PlotLines("index_join_pkfk", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_hash_join():
    ## hash join plots: 
    ## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: n1
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew~n1~n2"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        fname, w, h = "micro_{}_line_hashJoin.png".format(y_axis), 8, 8

        PlotLines("hash_join_pkfk", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
        
        lambda_function  = lambda x: x['n1'] == 10000000 and x['n2']==1000
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        fname, w, h = "micro_{}_10M_1k_line_hashJoin.png".format(y_axis), 6, 2.5

        PlotLines("hash_join_pkfk", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_aggs():
    for idx, y_axis in enumerate(y_axis_list):
        lambda_function  = lambda x: x['n1'] == 10000000
        x_axis, x_label, color, facet = "g", "Groups (g)", "system", None
        x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(header[idx])
        fname, w, h = "micro_{}_10M_line_reg_agg.png".format(y_axis), 4, 2.5

        PlotLines("reg_agg", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

plot_scans()
plot_filters()
plot_cross()
plot_ineq_joins()
plot_index_join()
plot_hash_join()
plot_aggs()
