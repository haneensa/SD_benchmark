import json
import pandas as pd
from pygg import *
import duckdb

type1 = ['1', '3', '5', '6', '7', '8', '9', '10', '12', '13', '14', '19']

type2 = ['11', '15', '16', '18']
type3 = ['2', '4', '17', '20', '21', '22']

con = duckdb.connect()

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
legend_none = legend + theme(**{"legend.position": esc("none")})

legend_side = legend + theme(**{
  "legend.position":esc("right"),
})

# for each query, 
def relative_overhead(base, extra): # in %
    return max(((float(extra)-float(base))/float(base))*100, 0)

def overhead(base, extra): # in ms
    return max(((float(extra)-float(base)))*1000, 0)

def getAllExec(plan, op):
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

def find_node_wprefix(prefix, plan):
    op_name = None
    if prefix[-4:] == "_mtm":
        prefix = prefix[:-4]
    for k, v in plan.items():
        if k[:len(prefix)] == prefix:
            op_name = k
            break
    return op_name

def getMat(plan, op):
    """
    return materialization time (sec) from
    profiling data stored in query plan
    """
    plan= plan.replace("'", "\"")
    plan = json.loads(plan)
    op = find_node_wprefix("CREATE_TABLE_AS", plan)
    if op == None:
        return 0
    return plan[op]

df = pd.read_csv("eval_results/tpch_benchmark_capture_sep23.csv")

header_unique = ["query","sf", "qtype", "lineage_type", "n_threads"]
g = ','.join(header_unique)
    
def getstats(row, i):
    if row["lineage_type"] == "SD_stats":
        return row['stats'].split(",")[i]
    else:
        0
df.loc[:, "lineage_size"] = df.apply(lambda x: getstats(x, 0), axis=1)
df.loc[:, "lineage_count"] = df.apply(lambda x: getstats(x, 1), axis=1)
df.loc[:, "nchunks"] = df.apply(lambda x: getstats(x, 2), axis=1)
df.loc[:, "postprocess"] = df.apply(lambda x: getstats(x, 3), axis=1)

df.loc[:, "mat_time"] = df.apply(lambda x: getMat(x['plan_timings'], x['query']), axis=1)
df.loc[:, "plan_runtime"] = df.apply(lambda x: getAllExec(x['plan_timings'], x['query']), axis=1)

def cat(qid):
    if qid in type1:
        return "1. Joins-Aggregations"
    elif qid in type2:
        return "2. Uncorrelated subQs"
    else:
        return "3. Correlated subQs"
df["qtype"] = df.apply(lambda x: cat(str(x['query'])), axis=1)

processed_data = pd.DataFrame(df).reset_index(drop=True)
file_name = "tpch.csv"
processed_data.to_csv(file_name, encoding='utf-8',index=False)
con.execute("CREATE TABLE tpch_test AS SELECT * FROM '{}';".format(file_name))
con.execute("COPY (SELECT * FROM tpch_test) TO '{}' WITH (HEADER 1, DELIMITER '|');".format(file_name))
# whenever opt does not exist for a query, use the same value as rid
print(con.execute("select count(*) from tpch_test").fetchdf())
print(con.execute("""create table aug_tpch as
                    select * from tpch_test UNION ALL
                    select query, runtime, sf, repeat, 'Logical-OPT' as lineage_type, n_threads, output, stats, notes, plan_timings,lineage_size, lineage_count, nchunks, postprocess, mat_time, plan_runtime, qtype from tpch_test
                    where lineage_type='Logical-RID'
                          and query not in (select query from tpch_test where lineage_type='Logical-OPT')
                   """.format(g, g)).fetchdf())
df = con.execute(f"""select * from aug_tpch""").fetchdf()
print(df)
pd.set_option("display.max_rows", None)


print(con.execute("""create table avg_tpch as
                    select {},
                            max(nchunks) as nchunks,
                            max(lineage_size) as lineage_size, max(lineage_count) as lineage_count,
                            max(postprocess) as postprocess,
                            avg(plan_runtime) as plan_runtime, avg(runtime) as runtime,
                            avg(output) as output,  avg(mat_time) as mat_time from aug_tpch
                            group by {}""".format(g, g)).fetchdf())
df = con.execute(f"""select * from avg_tpch""").fetchdf()
header_unique.remove("lineage_type")
metrics = ["runtime", "output", "mat_time", "plan_runtime", "lineage_size", "lineage_count", "nchunks", "postprocess"]
g = ','.join(header_unique)
m = ','.join(metrics)
print(con.execute(f"""create table tpch_withbaseline as select
                  t1.plan_runtime as base_plan_runtime, t1.runtime as base_runtime,
                  t1.output as base_output, t1.mat_time as base_mat_time,
                  (t1.plan_runtime-t1.mat_time) as base_plan_no_create,
                  (t2.plan_runtime-t2.mat_time) as plan_no_create,
                  t2.* from (select {g}, {m} from avg_tpch where lineage_type='Baseline') as t1 join avg_tpch  as t2 using ({g})
                  """))
df = con.execute(f"""select * from tpch_withbaseline""").fetchdf()
#print(df)
print(con.execute("""create table tpch_logical_metrics as select {}, lineage_type, output,
                            nchunks, lineage_size, lineage_count, postprocess,
                            0 as postprocess_roverhead,
                            (plan_no_create-base_plan_no_create)*1000 as plan_execution_overhead,
                            ((plan_no_create-base_plan_no_create)/base_plan_no_create)*100 as plan_execution_roverhead,
                            
                            (mat_time - base_mat_time)*1000 as plan_mat_overhead,
                            ((mat_time - base_mat_time) / base_plan_no_create) *100 as plan_mat_roverhead,

                            (plan_runtime-base_plan_runtime)*1000 as plan_all_overhead,
                            ((plan_runtime-base_plan_runtime)/base_plan_no_create)*100 as plan_all_roverhead,
                            
                            output / base_output as fanout
                  from tpch_withBaseline
                  where lineage_type='Logical-RID' or lineage_type='Logical-OPT' or lineage_type='Logical-window'
                  """.format(g)).fetchdf())

#print(con.execute("select * from tpch_logical_metrics").fetchdf())
con.execute("""create table tpch_sd_metrics as select {}, 'SD' as lineage_type, sd_full.output,
sd_stats.nchunks, sd_stats.lineage_size, sd_stats.lineage_count, sd_stats.postprocess,
(sd_stats.postprocess / (sd_full.base_plan_no_create*1000))*100 as postprocess_roverhead,
(sd_copy.plan_no_create-sd_copy.base_plan_no_create)*1000 as plan_execution_overhead,
((sd_copy.plan_no_create-sd_copy.base_plan_no_create)/sd_copy.base_plan_no_create)*100 as plan_execution_roverhead,
                            
(sd_full.plan_no_create - sd_copy.plan_no_create)*1000 as plan_mat_overhead,
((sd_full.plan_no_create - sd_copy.plan_no_create)/sd_copy.plan_no_create)*100 as plan_mat_roverhead,
                            
(sd_full.plan_no_create-sd_full.base_plan_no_create)*1000 as plan_all_overhead,
((sd_full.plan_no_create-sd_full.base_plan_no_create)/sd_full.base_plan_no_create)*100 as plan_all_roverhead,
                            
sd_full.output / sd_full.base_output as fanout
                     from (select * from tpch_withBaseline where lineage_type='SD_copy') as sd_copy JOIN
                          (select * from tpch_withBaseline where lineage_type='SD_full') as sd_full
                          USING ({}) JOIN
                          (select * from tpch_withBaseline where lineage_type='SD_stats') as sd_stats
                          USING ({})
                  """.format(g, g, g)).fetchdf()

#print(con.execute("select * from tpch_sd_metrics").fetchdf())


def mktemplate(overheadType, prefix, table):
    return f"""
    SELECT '{overheadType}' as overheadType, qtype,
            query as qid, sf, n_threads, output,
            nchunks, lineage_size, lineage_count, postprocess, postprocess_roverhead,
           lineage_type as System,
           greatest(0, {prefix}overhead) as overhead, greatest(0, {prefix}roverhead) as roverhead
    FROM {table}"""

template = f"""
  WITH data as (
    {mktemplate('Total', 'plan_all_', 'tpch_sd_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'plan_mat_', 'tpch_sd_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'plan_mat_', 'tpch_logical_metrics')}
    UNION ALL
    {mktemplate('Total', 'plan_all_', 'tpch_logical_metrics')}
    UNION ALL
    {mktemplate('Execute', 'plan_execution_', 'tpch_sd_metrics')}
    UNION ALL
    {mktemplate('Execute', 'plan_execution_', 'tpch_logical_metrics')}
  ) SELECT * FROM data {"{}"} ORDER BY overheadType desc """

def mktemplate2(table):
    return f"""
    SELECT  
            plan_execution_roverhead, plan_mat_roverhead, plan_all_roverhead,
            plan_execution_overhead, plan_mat_overhead, plan_all_overhead,
            query as qid, sf, n_threads, output,
           lineage_type as System
    FROM {table}"""

template2 = f"""
  WITH data as (
    {mktemplate2('tpch_sd_metrics')}
    UNION ALL
    {mktemplate2('tpch_logical_metrics')}
  ) SELECT * FROM data {"{}"} """


where = f"where overheadtype<>'Materialize' and sf<>20"
data = con.execute(template.format(where)).fetchdf()

class_list = type1
class_list.extend(type2)
class_list.extend(type3)
queries_order = [""+str(x)+"" for x in class_list]
queries_order = ','.join(queries_order)

if 1:
    y_axis_list = ["roverhead", "overhead"]
    header = ["Relative \nOverhead %", "Overhead (ms)"]
    for idx, y_axis in enumerate(y_axis_list):
        p = ggplot(data, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='overheadtype'))
        #p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.5, size=2)
        p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.8)
        p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
        p += legend_bottom
        p += legend_side
        p += facet_grid(".~sf~qtype", scales=esc("free_x"), space=esc("free_x"))
        postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
        ggsave("figures/tpch_{}.png".format(y_axis), p, postfix=postfix,  width=12, height=6, scale=0.8)
        data_sf = data[data['sf']==10] 
        p = ggplot(data_sf, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='overheadtype'))
        #p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.5, size=2)
        p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.8)
        p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
        p += legend_bottom
        p += legend_side
        p += facet_grid(".~qtype", scales=esc("free_x"), space=esc("free_x"))
        postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
        ggsave("figures/tpch_sf10_{}.png".format(y_axis), p, postfix=postfix,  width=14, height=2.75, scale=0.8)


if 0:
    queries_order = [""+str(x)+"" for x in range(1,23)]
    queries_order = ','.join(queries_order)
    p = ggplot(sd_data, aes(x='qid', y="size", fill="sf", group="sf"))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', "Size (MB) [log]", "discrete", "log10") + coord_flip()
    p += legend_bottom
    postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
    ggsave("tpch_metrics.png", p, postfix=postfix,  width=2.5, height=4)

# summarize
if 1:
    for sys in ["sd", "logical"]:
        q = f"""
        select lineage_type, qtype, sf, avg(plan_execution_roverhead), avg(plan_mat_roverhead), avg(plan_all_roverhead)
        from tpch_{sys}_metrics
        group by lineage_type, qtype, sf, n_threads
        order by qtype, sf, lineage_type
        """
        out = con.execute(q).fetchdf()
        #print(out)
        q = f"""
        select qtype, lineage_type, query, sf, output, fanout, plan_all_roverhead, plan_execution_roverhead, plan_mat_roverhead,
               postprocess, postprocess_roverhead
        from tpch_{sys}_metrics
        order by qtype, query, sf
        """
        out = con.execute(q).fetchdf()
        print(out)
