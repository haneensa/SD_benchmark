import numpy as np
import pandas as pd
from pygg import *
import json

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

# for each query, 
def overhead(sys, baseSys):
    return (sys-baseSys)

def relative_overhead(sys, baseSys, base):
    """
    Relative overhead: percentage increase in execution time
        caused by the operation  compared to some baseline.
    sys (uint) execution of system of interest
    baseSys (uint) baseline execution
    base (uint) runtime of baseline execution

    (sys-baseSys) -> overhead of system compared to baseline
    """
    h = max(((sys-baseSys)/base)*100, 0)
    return h

def getBase(plan, op, sys):
    """
    given a query plan and an operator from duckdb,
    return the operator name in the query plan
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
    
lcopy = "a12_copy"
lfull = "a12_full"

df_all = pd.read_csv("eval_results/micro_benchmark_notes_a12.csv")
df_all["card2"] = 0
df_all["sel"] = 0

df_logical = df_all[df_all["notes"]=="a12"]
df_logical["notes"] = "logical"
df_stats = df_all[df_all["notes"]=="a12_stats"]#pd.read_csv("eval_results/micro_benchmark_notes_feb26b_stats.csv")
df_full = df_all[df_all["notes"]=="a12_full"]
df_copy = df_all[df_all["notes"]=="a12_copy"]

df_data = df_logical
df_data = df_data.append(df_full)
df_data = df_data.append(df_copy)
df_data["baseExec"] = df_data.apply(lambda x: getBase(x['plan_timings'], x['query'], x["lineage_type"]), axis=1)
df_data["mat"] = df_data.apply(lambda x: getMat(x['plan_timings'], x['query'], x["lineage_type"]), axis=1)
df_data["allExec"] = df_data.apply(lambda x: getAllExec(x['plan_timings'], x['query'], x["lineage_type"]), axis=1)
df_data["allExcept"] = df_data.apply(lambda x: max(x["allExec"] - x["mat"], 0.0), axis=1)
pd.set_option("display.max_rows", None)



# 1. Partition by query time [orderby, filter, filter_scan, perfect_agg, reg_agg, groupby
#   a. Order by: vary cardinality (1M, 5M, 10M)
#   b. Filter & Filter Scan: vary cardinality (1M, 5M, 10M), vary selectivity: 0, 0.2, ,0.5, 1.0
#   c. aggs: vary cardinality, vary number of groups
#   d. merge, nlj, bnlj, cross product: vary cardinality of two tables
#   e. hash_join pkfk
#   f. index_join pkfk: 

# separate materialization overhead and execution overhead and plot them as stacked bars
# x-axis groups (e.g. cardinality, selectivity, projection)
# y-axis overhead in (ms) stacked for materialization and execution
# facet: system
def PlotSelect(filterType):
    # TODO: include materialization costs and operator execution cost and whole plan execution cost and whole plan without that specific operator
    print("****************** Summary for : ", filterType)
    df = df_data[df_data['query'] == filterType]
    df_statsq = df_stats[df_stats['query'] == filterType]

    df = df.drop(columns=["query", "stats"])
    df_Baseline = df[df["lineage_type"]=="Baseline"]
    df_Baseline = df_Baseline[["cardinality", "groups", "runtime", "output", "p", "baseExec", "r", "mat", "allExcept"]]
    df_Baseline = df_Baseline.rename({'runtime':"Bruntime", 'output': 'Boutput', 'baseExec':'BbaseExec', 'mat':'Bmat', "allExcept":"BallExcept"}, axis=1)
    df= df[df["lineage_type"]!="Baseline"]
    df_logical= df[df["lineage_type"]=="Perm"]
    df_sd= df[df["lineage_type"]=="SD_Capture"]
    # compute overhead for logical
    # OverheadLog =(runtime of all operators in the rewritten plan + mat) - (runtime of all operators in the base query + mat)
    df_LogicalwithB = pd.merge(df_logical, df_Baseline, how='inner', on = ['cardinality', "groups", "p", "r"])
    
    df_LogicalwithB["roverhead"] = df_LogicalwithB.apply(lambda x: relative_overhead(x['runtime'], x['Bruntime'], x['BbaseExec']), axis=1)
    df_LogicalwithB["overhead"] = df_LogicalwithB.apply(lambda x: overhead(x['runtime'], x['Bruntime']), axis=1)
    df_LogicalwithB["fanout"] = df_LogicalwithB.apply(lambda x: fanout(x['output'],float(x['Boutput'])), axis=1)
    df_LogicalwithB["matOverhead"] = df_LogicalwithB.apply(lambda x: (x["mat"] - x['Bmat']), axis=1)
    df_LogicalwithB["execOverhead"] = df_LogicalwithB.apply(lambda x:( x["allExcept"] - x['BallExcept']), axis=1)
    
    df_LogicalwithB["matROverhead"] = df_LogicalwithB.apply(lambda x: max((x["matOverhead"] / x["BbaseExec"])*100, 0), axis=1)
    df_LogicalwithB["execROverhead"] = df_LogicalwithB.apply(lambda x: max((x["execOverhead"] / x["BbaseExec"])*100, 0), axis=1)
    
    df_sdwithB = pd.merge(df_sd, df_Baseline, how='inner', on = ['cardinality', "groups", "p", "r"])
    
    df_sdwithB["roverhead"] = df_sdwithB.apply(lambda x: relative_overhead(x['baseExec'], x['BbaseExec'], x['BbaseExec']), axis=1)
    df_sdwithB["overhead"] = df_sdwithB.apply(lambda x: overhead(x['baseExec'], x['BbaseExec']), axis=1)
    df_sdwithB["fanout"] = df_sdwithB.apply(lambda x: fanout(x['output'],float(x['Boutput'])), axis=1)
    df_withB = df_sdwithB
    df_withB = df_withB.append(df_LogicalwithB)
    perm_select = ["overhead", "fanout", "roverhead", "runtime", "Bruntime", "matOverhead", "execOverhead", "matROverhead", "execROverhead"] 
    sd_select = ["overhead", "fanout", "roverhead", "runtime", "Bruntime"] 
    if filterType == "index_join_pkfk":

        df_withB["card2"] = df_withB.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        df_statsq["card2"] = df_statsq.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        
        df_withB["sel"] = df_withB.apply(lambda x:  round(1.0 - float(x["groups"].split(",")[1]), 1), axis=1)
        df_statsq["sel"] = df_statsq.apply(lambda x:  round(1.0 -  float(x["groups"].split(",")[1]), 1), axis=1)
        
        df_withB["a"] = df_withB.apply(lambda x:  x["groups"].split(",")[2], axis=1)
        df_statsq["a"] = df_statsq.apply(lambda x:  x["groups"].split(",")[2], axis=1)
        
        df_withB["index_scan"] = df_withB.apply(lambda x:  x["groups"].split(",")[3], axis=1)
        df_statsq["index_scan"] = df_statsq.apply(lambda x:  x["groups"].split(",")[3], axis=1)
        
        df_withB["groups"] = df_withB.apply(lambda x:  x["a"], axis=1)
        df_statsq["groups"] = df_statsq.apply(lambda x: x["a"], axis=1)

        df_withB["p"] = df_withB.apply(lambda x: "Qidx" if x["index_scan"] == "False" else "Qno_idx", axis=1)
        df_statsq["p"] = df_statsq.apply(lambda x:  "Qidx" if x["index_scan"] == "False" else "Qno_idx", axis=1)
        
        df_withB["lineage_type_temp"] = df_withB.apply(lambda x: x["lineage_type"] + x["p"], axis=1)
        df_statsq["lineage_type_temp"] = df_statsq.apply(lambda x: x["lineage_type"] + x["p"], axis=1)
        
        ops = ["Qidx", "Qno_idx"]
        for o in ops:
            perm_overhead = df_withB[df_withB["lineage_type_temp"]=="Perm"+o].aggregate(["mean", "min", "max"])
            print(o, " Perm: ", perm_overhead[ perm_select ]) 
            
            sd_overhead = df_withB[df_withB["lineage_type_temp"]=="SD_Capture"+o].groupby(["notes"]).aggregate(["mean", "min", "max"])
            print(o, " SD_Capture: ", sd_overhead[ sd_select ]) 
            print(o, " Speedup: ", perm_overhead['roverhead'] / sd_overhead['roverhead'])
            perm_overhead = df_withB[df_withB["lineage_type_temp"]=="Perm"+o].groupby(["card2", "cardinality","groups","p", "sel"]).aggregate(["mean"])
            sd_overhead = df_withB[df_withB["lineage_type_temp"]=="SD_Capture"+o].groupby(["card2", "cardinality","groups","p", "notes", "sel"]).aggregate(["mean"])
            m = pd.merge(perm_overhead[ perm_select ],  sd_overhead[ sd_select ], how='inner', on=["card2", 'cardinality', 'groups', 'p', "sel"]) 
            print(m)
    elif filterType in ["cross", "nl", "bnl", "merge"]:
        df_withB["card2"] = df_withB.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        df_statsq["card2"] = df_statsq.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        
        df_withB["sel"] = df_withB.apply(lambda x:  "{}".format(round(1.0 - float(x["groups"].split(",")[1]), 1)), axis=1)
        df_statsq["sel"] = df_statsq.apply(lambda x:  "{}".format(round(1.0 -  float(x["groups"].split(",")[1]), 1)), axis=1)
        
        perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].aggregate(["mean", "min", "max"])
        print("Perm: ", perm_overhead[ perm_select ]) 
        
        sd_overhead = df_withB[df_withB["lineage_type"]=="SD_Capture"].groupby(["notes"]).aggregate(["mean", "min", "max"])
        print("SD_Capture: ", sd_overhead[ sd_select ]) 
        print("Speedup: ", perm_overhead['roverhead'] / sd_overhead['roverhead'])
        
        perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].groupby(["cardinality","groups","p", "notes"]).aggregate(["mean"])
        sd_overhead = df_withB[df_withB["lineage_type"]=="SD_Capture"].groupby(["cardinality","groups","p", "notes"]).aggregate(["mean"])
        m = pd.merge(perm_overhead[ perm_select ],  sd_overhead[ sd_select ], how='inner', on=['cardinality', 'groups', 'p']) 
        print(m)
    else:
        perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].aggregate(["mean", "min", "max"])
        print("Perm: ", perm_overhead[ perm_select ]) 
        
        sd_overhead = df_withB[df_withB["lineage_type"]=="SD_Capture"].groupby(["notes"]).aggregate(["mean", "min", "max"])
        print("SD_Capture: ", sd_overhead[ sd_select ]) 
        print("Speedup: ", perm_overhead['roverhead'] / sd_overhead['roverhead'])
        
        perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].groupby(["cardinality","groups","p", "notes"]).aggregate(["mean"])
        sd_overhead = df_withB[df_withB["lineage_type"]=="SD_Capture"].groupby(["cardinality","groups","p", "notes"]).aggregate(["mean"])
        m = pd.merge(perm_overhead[ perm_select ],  sd_overhead[ sd_select ], how='inner', on=['cardinality', 'groups', 'p']) 
        print(m)
    
    if filterType == "hash_join_pkfk":
        df_withB["g"] = df_withB.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        df_withB["card2"] = df_withB.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        df_withB["sel"] = df_withB.apply(lambda x:  round(1.0 - float(x["groups"].split(",")[1]), 1), axis=1)
        df_withB["a"] = df_withB.apply(lambda x:  x["groups"].split(",")[2], axis=1)
        df_withB["groups"] = df_withB.apply(lambda x:  "{}/{}".format(x["sel"],x["a"]), axis=1)
        df_withB["p"] = df_withB.apply(lambda x:  x["a"], axis=1)
        df_statsq["sel"] = df_statsq.apply(lambda x:  round(1.0 -  float(x["groups"].split(",")[1]), 1), axis=1)
        
        df_statsq["g"] = df_statsq.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        df_statsq["card2"] = df_statsq.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        df_statsq["sel"] = df_statsq.apply(lambda x:  round(1.0 - float(x["groups"].split(",")[1]), 1), axis=1)
        df_statsq["a"] = df_statsq.apply(lambda x:  x["groups"].split(",")[2], axis=1)
        df_statsq["groups"] = df_statsq.apply(lambda x:  "{}/{}".format(x["sel"],x["a"]), axis=1)
        df_statsq["p"] = df_statsq.apply(lambda x:  x["a"], axis=1)
    elif filterType in ["filter", "filter_scan", "scan", "orderby"]:
        df_withB["p"] = df_withB.apply(lambda x:  x["p"]+3, axis=1)
        df_statsq["p"] = df_statsq.apply(lambda x:  x["p"]+3, axis=1)
        
    df_withBAggs = df_withB.groupby(["sel", "card2", "cardinality", "groups", "p", "lineage_type", "notes"]).aggregate(["mean"]).droplevel(axis=1, level=1).reset_index()


    # full = copy + capture
    # noCapture = copy
    # capture = full - copy
    #print(df_withB.groupby(['cardinality', 'groups', 'lineage_type_x', 'notes_x']).mean())
    df_copy = df_withBAggs[df_withBAggs['notes'] == lcopy]
    df_full = df_withBAggs[df_withBAggs['notes'] == lfull]
    
    df_full = df_full.drop(columns=["lineage_type", "output", "fanout"])
    
    df_full = df_full[["sel", "card2", "roverhead", "overhead", "runtime", "cardinality", "groups", "p", "BbaseExec"]]
    df_copy = df_copy[["sel", "card2", "roverhead", "overhead", "runtime", "cardinality", "groups", "p"]]
    
    df_full = df_full.rename({'roverhead': 'roverhead_f','overhead':'overhead_f', 'runtime':"runtime_f" }, axis=1)
    df_copy = df_copy.rename({'roverhead': 'roverhead_c','overhead':'overhead_c', 'runtime':"runtime_c" }, axis=1)
    
    df_fc = pd.merge(df_copy, df_full, how='inner', on = ["sel", "card2", 'cardinality', "groups", "p"])

    def normalize(full, nchunks):
        if nchunks == 0:
            return full
        else:
            return full/nchunks
    
    df_statsq = df_statsq[["sel", "card2", "stats", "cardinality", "groups", "p", "r"]]#.drop(columns=["notes", "output", "runtime"])
    df_fcstats = pd.merge(df_fc, df_statsq, how='inner', on = ["sel", "card2", 'cardinality', "groups", "p"])

    df_fcstats["foverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_f'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["coverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_c'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["froverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['roverhead_f'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["croverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['roverhead_c'],float(x['stats'].split(',')[1])), axis=1)

    df_fcstats["size"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[0])/(1024.0*1024.0), axis=1)
    df_fcstats["nchunks"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[1]), axis=1)
    df_fcstats = df_fcstats.drop(columns=["stats"])

    for index, row in df_withBAggs.iterrows():
        card = row["cardinality"]
        groups = row["groups"]
        p = row["p"]
        ltype = row["lineage_type"]
        mat_over = 0
        over = 0
        if ltype == "Perm":
            ltype = "Logical"
            mat_over = row["matOverhead"]
            over = row["execOverhead"]
            mat_over_R = row["matROverhead"]
            over_R = row["execROverhead"]
            rel_overhead = row["roverhead"]
            card_g = "{}/{}".format(card, groups)
            p_g = "{}/{}".format(p, groups)
            p_card = "{}/{}".format(p, card)
            ltype_a = "{}_a{}".format(ltype, p)
            op_ltype = filterType+ltype
            card2 = row['card2']
            sel = row['sel']
            data.append(dict(overheadType="mat", sel=sel, card2=card2, card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=mat_over_R, overhead=mat_over*1000,  optype=filterType))
            data.append(dict(overheadType="exec", sel=sel, card2=card2, card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=over_R, overhead=over*1000,  optype=filterType))

    for index, row in df_fc.iterrows():
        card = row["cardinality"]
        groups = row["groups"]
        p = row["p"]
        ltype = "SD"
        copy_over = max(row["overhead_c"], 0)
        copy_over_relative = (copy_over / row["BbaseExec"])*100
        logging_over = max(row["overhead_f"] - copy_over, 0)
        logging_over_relative = (logging_over / row["BbaseExec"])*100
        card2 = row['card2']
        sel = row['sel']
        #print(logging_over, copy_over, card2, sel)
        # add copy overhead as overhead and full as materialization overhead?

        rel_overhead = row["roverhead_f"]

        card_g = "{}/{}".format(card, groups)
        p_g = "{}/{}".format(p, groups)
        p_card = "{}/{}".format(p, card)
        ltype_a = "{}_a{}".format(ltype, p)
        op_ltype = filterType+ltype
        data.append(dict(overheadType="exec", sel=sel, card2=card2, card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=copy_over_relative, overhead=copy_over*1000,  optype=filterType))
        data.append(dict(overheadType="mat", sel=sel, card2=card2, card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=logging_over_relative, overhead=logging_over*1000,  optype=filterType))

cardinality_str = ["1M", "5M", "10M"]
selections_str = ["0.0", "0.2", "0.5", "1.0"]
group_order= ["'{}/{}'".format(a, b) for a in cardinality_str for b in selections_str]
group_order = ','.join(group_order)

postfix = "data$card= factor(data$card, levels=c({}))".format(group_order)
# Reorder the category variable based on the defined group order

y_axis_list = ["roverhead", "overhead"]
header = ["Relative\nOverhead %", "Overhead (ms)"]
    
'''
proj_str = [3, 5, 7, 11]
selections_str = [0.02, 0.2, 0.5, 1.0]
group_order= ['"{}/\'{}\'"'.format(a, b) for a in proj_str for b in selections_str]
group_order = ','.join(group_order)
gorder = """data$p_g = factor(data$p_g, levels=c({}))""".format(group_order)
# plot runtime for Baseline



#### cross
# vary cardinality of n2
# both systems dominated by materializations overhead
data = []
PlotSelect("cross")
cross_data = data
for idx, y_axis in enumerate(y_axis_list):
    filtered_cross_data = cross_data
    if y_axis == 'roverhead':
        filtered_cross_data = [d for d in cross_data if d[label] == "exec"]
        linetype = None
    else:
        linetype = label

    pdata = filtered_cross_data
    x_axis, x_label = "card2", "Cardinality"
    x_type, y_type, y_label = "continuous", "log10",  "{} [log]".format(header[idx])
    color, facet = "system", None
    fname = "micro_{}_cross.png".format(y_axis)
    w, h = 5.5, 2.5
    PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

##### Hash Agg
# vary cardinality and groups
# varying cardinality: same relative overhead across
# varying groups: perm execution overhead increasese as groups decreases for the same cardinality
data = []
PlotSelect("perfect_agg")
PlotSelect("reg_agg")
agg_data = data
## aggs
## 1) X-axis: groups | skew, Y-axis: relative overhead | overhead; group: cardinality
for y_axis in y_axis_list:
    p = ggplot(agg_data, aes(x='card_g', y=y_axis, fill=label))
    p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)
    p += axis_labels('Carinality (n)/ groups (g)', "{}".format(y_axis), "discrete")  + coord_flip() 
    p += facet_wrap("~system~optype", scales=esc("free_x"))
    ggsave("micro_{}_agg.png".format(y_axis), p,  postfix=gorder, width=10, height=6)

for idx, y_axis in enumerate(y_axis_list):
    filtered_agg_data = [d for d in agg_data if d['cardinality'] == 10000000 and d['optype']=="reg_agg"]
    linetype = label

    pdata = filtered_agg_data
    x_axis, x_label, y_label = "g", "Groups (g)", "{} [log]".format(header[idx])
    x_type, y_type = "continuous", "log10"
    color, facet = "system", None
    fname = "micro_{}_10M_line_reg_agg.png".format(y_axis)
    w, h = 4, 2.5

    PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

##### Equality Joins -- hash join
data = []
PlotSelect("hash_join_pkfk")
equi_join = data
for d in equi_join:
    d["p"] = "Skew: {}".format(d["p"])

## hash join plots: 
## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: cardinality
for idx, y_axis in enumerate(y_axis_list):
    filtered_equi_join = [d for d in equi_join]
    linetype = label

    pdata = filtered_equi_join
    x_axis, x_label = "sel", "Selectivity"
    x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
    #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
    color, facet = "system", "~p~cardinality~card2"
    fname = "micro_{}_line_hashJoin.png".format(y_axis)
    w, h = 8, 8

    PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
    
    filtered_equi_join = [d for d in filtered_equi_join if d['cardinality'] == 10000000 and d["card2"]=='1000']
    pdata = filtered_equi_join
    x_axis, x_label = "sel", "Selectivity"
    #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
    x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
    color, facet = "system", "~p"
    fname = "micro_{}_10M_1k_line_hashJoin.png".format(y_axis)
    w, h = 6, 2.5

    PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

##### In-Equality Joins
# vary n2 and selectivity of the join condition
data = []
PlotSelect("nl")
PlotSelect("merge")
PlotSelect("bnl")
ineq_joins = data

# joins inequiality
## 1) X-axis: selectivity Y-axis: relative overhead | overhead; group: cardinality
for idx, y_axis in enumerate(y_axis_list):
    filtered_ineq_joins = [d for d in ineq_joins]
    linetype = label

    pdata = filtered_ineq_joins
    x_axis, x_label = "sel", "Selectivity"
    #x_type, y_type, y_label = "continuous",  x_type, "{}".format(header[idx])
    x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(header[idx])
    color, facet = "system", "~optype~card2"
    fname = "micro_{}_line_ineq.png".format(y_axis)
    w, h = 8, 4
    PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

for idx, y_axis in enumerate(y_axis_list):
    filtered_ineq_joins = [d for d in ineq_joins if d['card2'] == '1000000']
    linetype = label

    pdata = filtered_ineq_joins
    x_axis, x_label = "sel", "Selectivity"
    #x_type, y_type, y_label = "continuous",  x_type, "{}".format(header[idx])
    x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(header[idx])
    color, facet = "system", "~optype"
    fname = "micro_{}_10M_1k_line_ineq.png".format(y_axis)
    w, h = 6, 2.5
    PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

##### Scans
data = []
simple_data = {}
PlotSelect("scan")
simple_data["scans"]= data

data = []
PlotSelect("orderby")
simple_data["orderby"]= data
data = []
PlotSelect("filter")
simple_data["filter"]= data
data = []
PlotSelect("filter_scan")
simple_data["filter_scan"]= data
data = []

## filter plots: 
## 1) X-axis: selectivity | projection, Y-axis: relative overhead | overhead; facet: cardinality | system
ops = ["filter", "filter_scan"]
for op in ops:
    for idx, y_axis in enumerate(y_axis_list):
        filtered_data = simple_data[op]
        linetype = label

        pdata = filtered_data
        x_axis, x_label = "g", "Selectivity"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        color, facet = "system", "~cardinality~p"
        fname = "micro_{}_line_{}.png".format(y_axis, op)
        w, h = 8, 4

        PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

        linetype = label

        pdata = filtered_data
        x_axis, x_label = "p", "Projected Columns"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        color, facet = "system", "~cardinality~g"
        fname = "micro_{}_projection_line_{}.png".format(y_axis, op)
        w, h = 8, 4

        PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

        filtered_data = [d for d in simple_data[op] if d['cardinality'] == 10000000 and d['p']==3]
        linetype = label

        pdata = filtered_data
        x_axis, x_label= "g", "Selectivity",
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        color, facet = "system", None#"~p"
        fname = "micro_{}_10M_p3_line_{}.png".format(y_axis, op)
        w, h = 5, 2.5

        PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

## filter plots: 
## 1) X-axis: selectivity | projection, Y-axis: relative overhead | overhead; facet: cardinality | system
ops = ["scans", "orderby"]
for op in ops:
    for idx, y_axis in enumerate(y_axis_list):
        filtered_data = simple_data[op]
        linetype = label

        pdata = filtered_data
        x_axis, x_label = "cardinality", "#col / Selectivity"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        color, facet = "system", "~p"
        fname = "micro_{}_line_{}.png".format(y_axis, op)
        w, h = 8, 3

        PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)


'''

def PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, wrap=None):
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

label = "overheadType"
gorder = """data${} = factor(data${}, levels=c("mat", "exec"))""".format(label, label, label)

##### Index Join
data = []
PlotSelect("index_join_pkfk")
index_join = data
for d in index_join:
    d["g"] = "Skew: {}".format(d["g"])

## index join plots: 
## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: cardinality
ps = ["Qidx", "Qno_idx"]
ps_2 = ["Q_P,F", "Q_P"]
for d in index_join:
    if d["p"] == "Qidx":
        d["p"] = ps_2[0]
    else:
        d["p"] = ps_2[1]
ps = ps_2
for p in ps:
    for idx, y_axis in enumerate(y_axis_list):
        filtered_index_join = [d for d in index_join]
        linetype = label
        pdata = filtered_index_join
        x_axis, x_label = "sel", "Selectivity"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        color, facet = "system", "~p~g~cardinality~card2"
        fname = "micro_{}_line_indexJoin.png".format(y_axis)
        w, h = 8, 10

        PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
        
        filtered_index_join = [d for d in filtered_index_join if d['cardinality'] == 10000000 and d['card2']=='1000' and d["p"]==p]

        pdata = filtered_index_join
        x_axis, x_label = "sel", "Selectivity"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(header[idx])
        color, facet = "system", "~system~p~g"
        fname = "micro_{}_{}_10M_1k_line_indexJoin.png".format(y_axis, p)
        w, h = 6, 3

        PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, "p")
    break

