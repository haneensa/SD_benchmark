import numpy as np
import pandas as pd
from pygg import *
import json
# for each query, 
def overhead(sys, baseSys):
    return (sys-baseSys)*1000

def relative_overhead(sys, baseSys, base):
    h = max(((sys-baseSys)/base)*100, 0)
    #print(h, sys, baseSys, base)
    return h

def getBase(plan, op, sys):
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
    plan= plan.replace("'", "\"")
    plan = json.loads(plan)
    total = 0.0
    for k, v in plan.items():
        total += float(v)
    return total

def getMat(plan, op, sys):
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

def PlotSelect(filterType):
    # TODO: include materialization costs and operator execution cost and whole plan execution cost and whole plan without that specific operator
    print("****************** Summary for : ", filterType)
    df = df_data[df_data['query'] == filterType]
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
    df_LogicalwithB["matOverhead"] = df_LogicalwithB.apply(lambda x: (x["mat"] - x['Bmat'])*1000, axis=1)
    df_LogicalwithB["execOverhead"] = df_LogicalwithB.apply(lambda x:( x["allExcept"] - x['BallExcept'])*1000, axis=1)
    print(df_LogicalwithB)
    
    df_sdwithB = pd.merge(df_sd, df_Baseline, how='inner', on = ['cardinality', "groups", "p", "r"])
    
    df_sdwithB["roverhead"] = df_sdwithB.apply(lambda x: relative_overhead(x['baseExec'], x['BbaseExec'], x['BbaseExec']), axis=1)
    df_sdwithB["overhead"] = df_sdwithB.apply(lambda x: overhead(x['baseExec'], x['BbaseExec']), axis=1)
    df_sdwithB["fanout"] = df_sdwithB.apply(lambda x: fanout(x['output'],float(x['Boutput'])), axis=1)
    df_withB = df_sdwithB
    print("Y", df_withB.columns)
    df_withB = df_withB.append(df_LogicalwithB)
    print("X", df_withB.columns)
    perm_select = ["overhead", "fanout", "roverhead", "runtime", "Bruntime", "matOverhead", "execOverhead"] 
    sd_select = ["overhead", "fanout", "roverhead", "runtime", "Bruntime"] 
    if filterType == "index_join_pkfk":
        df_withB["lineage_type_temp"] = df_withB.apply(lambda x: x["lineage_type"] + x["groups"].split(",")[3], axis=1)
        df_withB["g"] = df_withB.apply(lambda x:  x["groups"].split(",")[0], axis=1)
        df_withB["sel"] = df_withB.apply(lambda x:  x["groups"].split(",")[1], axis=1)
        df_withB["a"] = df_withB.apply(lambda x:  x["groups"].split(",")[2], axis=1)
        df_withB["index_scan"] = df_withB.apply(lambda x:  x["groups"].split(",")[3], axis=1)
        df_withB["groups"] = df_withB.apply(lambda x:  x["g"]+"/"+x["sel"], axis=1)
        df_withB["p"] = df_withB.apply(lambda x:  x["index_scan"], axis=1)
        ops = ["False", "True"]
        for o in ops:
            perm_overhead = df_withB[df_withB["lineage_type_temp"]=="Perm"+o].aggregate(["mean", "min", "max"])
            print(o, " Perm: ", perm_overhead[ perm_select ]) 
            
            sd_overhead = df_withB[df_withB["lineage_type_temp"]=="SD_Capture"+o].groupby(["notes"]).aggregate(["mean", "min", "max"])
            print(o, " SD_Capture: ", sd_overhead[ sd_select ]) 
            print(o, " Speedup: ", perm_overhead['roverhead'] / sd_overhead['roverhead'])
            perm_overhead = df_withB[df_withB["lineage_type_temp"]=="Perm"+o].groupby(["cardinality","groups","p"]).aggregate(["mean"])
            sd_overhead = df_withB[df_withB["lineage_type_temp"]=="SD_Capture"+o].groupby(["cardinality","groups","p", "notes"]).aggregate(["mean"])
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
        df_withB["sel"] = df_withB.apply(lambda x:  x["groups"].split(",")[1], axis=1)
        df_withB["a"] = df_withB.apply(lambda x:  x["groups"].split(",")[2], axis=1)
        df_withB["groups"] = df_withB.apply(lambda x:  x["g"]+"/"+x["sel"], axis=1)
        df_withB["p"] = df_withB.apply(lambda x:  x["a"], axis=1)
    df_withBAggs = df_withB.groupby(["cardinality", "groups", "p", "lineage_type", "notes"]).aggregate(["mean"]).droplevel(axis=1, level=1).reset_index()

    print("J", df_withB.columns)

    # full = copy + capture
    # noCapture = copy
    # capture = full - copy
    #print(df_withB.groupby(['cardinality', 'groups', 'lineage_type_x', 'notes_x']).mean())
    df_copy = df_withBAggs[df_withBAggs['notes'] == lcopy]
    df_full = df_withBAggs[df_withBAggs['notes'] == lfull]
    
    df_full = df_full.drop(columns=["lineage_type", "output", "fanout"])
    
    df_full = df_full[["roverhead", "overhead", "runtime", "cardinality", "groups", "p"]]
    df_copy = df_copy[["roverhead", "overhead", "runtime", "cardinality", "groups", "p"]]
    
    df_full = df_full.rename({'roverhead': 'roverhead_f','overhead':'overhead_f', 'runtime':"runtime_f" }, axis=1)
    df_copy = df_copy.rename({'roverhead': 'roverhead_c','overhead':'overhead_c', 'runtime':"runtime_c" }, axis=1)
    
    df_fc = pd.merge(df_copy, df_full, how='inner', on = ['cardinality', "groups", "p"])

    def normalize(full, nchunks):
        if nchunks == 0:
            return full
        else:
            return full/nchunks
    
    df_statsq = df_stats[df_stats['query'] == filterType]
    df_statsq = df_statsq[["stats", "cardinality", "groups", "p", "r"]]#.drop(columns=["notes", "output", "runtime"])
    df_fcstats = pd.merge(df_fc, df_statsq, how='inner', on = ['cardinality', "groups"])

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
            rel_overhead = row["roverhead"]
            card_g = "{}/{}".format(card, groups)
            p_g = "{}/{}".format(p, groups)
            p_card = "{}/{}".format(p, card)
            ltype_a = "{}_a{}".format(ltype, p)
            op_ltype = filterType+ltype
            data.append(dict(overheadType="exec", card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=rel_overhead, overhead=over,  optype=filterType))
            data.append(dict(overheadType="mat", card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=rel_overhead, overhead=mat_over,  optype=filterType))

    for index, row in df_fc.iterrows():
        card = row["cardinality"]
        groups = row["groups"]
        p = row["p"]
        ltype = "SmokedDuck"
        over = row["overhead_c"]
        mat_over = row["overhead_f"] - mat_over
        print(mat_over, over)
        # add copy overhead as overhead and full as materialization overhead?

        rel_overhead = row["roverhead_f"]

        card_g = "{}/{}".format(card, groups)
        p_g = "{}/{}".format(p, groups)
        p_card = "{}/{}".format(p, card)
        ltype_a = "{}_a{}".format(ltype, p)
        op_ltype = filterType+ltype
        data.append(dict(overheadType="mat", card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=rel_overhead, overhead=over,  optype=filterType))
        data.append(dict(overheadType="exec", card_g=card_g, p_g=p_g, p_card=p_card, ltype_a=ltype_a, op_ltype=op_ltype, system=ltype,p=p, g=groups, cardinality=card, roverhead=rel_overhead, overhead=mat_over,  optype=filterType))

cardinality_str = ["1M", "5M", "10M"]
selections_str = ["0.0", "0.2", "0.5", "1.0"]
group_order= ["'{}/{}'".format(a, b) for a in cardinality_str for b in selections_str]
group_order = ','.join(group_order)

group_label = "overheadType"
postfix = "data$card= factor(data$card, levels=c({}))".format(group_order)
y_axis_list = ["roverhead", "overhead"]
header = ["Relative Overhead %", "Runtime Overhead (ms)"]
    

##### Scans
'''
data = []
simple_data = {}
PlotSelect("scan")
simple_data["scans"]= data

data = []
PlotSelect("orderby")
simple_data["orderby"]= data
data = []
PlotSelect("filter")
PlotSelect("filter_scan")
simple_data["filter"]= data
data = []

## filter plots: 
## 1) X-axis: selectivity | projection, Y-axis: relative overhead | overhead; group: cardinality
ops = ["scans", "filter", "orderby"]
for op in ops:
    for idx, y_axis in enumerate(y_axis_list):
        groups =  'p_card' if op == "scans" else  'p_g'
        label = "system"# if op == "scans" else group_label
        p = ggplot(simple_data[op], aes(x=groups, y=y_axis, color=label, fill=label, group=label, shape=label))
        p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)
        if (op != "scans"):
            p += axis_labels('#col / Selectivity', "{}".format(header[idx]), "discrete")  + coord_flip() 
            p += facet_wrap("~cardinality~optype", scales=esc("free_x"))
            ggsave("micro_{}_{}.png".format(y_axis, op), p,  width=10, height=6)
        else:
            p += axis_labels('# col / # cardinality', "{}".format(header[idx]), "discrete")  + coord_flip() 
            ggsave("micro_{}_{}.png".format(y_axis, op), p,  width=6, height=4)

##### Hash Agg
data = []
PlotSelect("perfect_agg")
PlotSelect("reg_agg")
agg_data = data
## aggs
## 1) X-axis: groups | skew, Y-axis: relative overhead | overhead; group: cardinality
for y_axis in y_axis_list:
    p = ggplot(agg_data, aes(x='card_g', y=y_axis, color=group_label, fill=group_label, group=group_label, shape=group_label))
    p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)
    p += axis_labels('Carinality (n)/ groups (g)', "{}".format(y_axis), "discrete")  + coord_flip() 
    p += facet_wrap("~system~optype", scales=esc("free_x"))
    ggsave("micro_{}_agg.png".format(y_axis), p,  width=10, height=6)

data = []
PlotSelect("cross")
cross_data = data
for y_axis in y_axis_list:
    p = ggplot(cross_data, aes(x='card_g', y=y_axis, color=group_label, fill=group_label, group=group_label, shape=group_label))
    p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)
    p += axis_labels('Carinality (n1)/(n2)', "{}".format(y_axis), "discrete")+ coord_flip() 
    p += facet_wrap("~system", scales=esc("free_x"))
    ggsave("micro_{}_cross.png".format(y_axis), p,  width=10, height=3)

##### In-Equality Joins
data = []
PlotSelect("nl")
PlotSelect("merge")
PlotSelect("bnl")
ineq_joins = data
# joins inequiality
## 1) X-axis: selectivity Y-axis: relative overhead | overhead; group: cardinality
for y_axis in y_axis_list:
    p = ggplot(ineq_joins, aes(x='g', y=y_axis, fill='overheadType', group='overheadType'))
    p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)
    p += axis_labels('Cardinality/Selectivity', "{}".format(y_axis), "discrete")
    p += facet_wrap("~cardinality~optype~system", scales=esc("free_y"))
    ggsave("micro_{}_ineq.png".format(y_axis), p,  width=30, height=4)


##### Equality Joins -- index join
data = []
PlotSelect("hash_join_pkfk")
equi_join = data

## hash join plots: 
## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: cardinality
for y_axis in y_axis_list:
    p = ggplot(equi_join, aes(x='g', y=y_axis, color="ltype_a", fill="ltype_a", group="ltype_a", shape="ltype_a"))
    p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)
    p += axis_labels('Selectivity', "{}".format(y_axis), "discrete") + coord_flip() 
    p += facet_wrap("~cardinality", scales=esc("free_y"))
    ggsave("micro_{}_equiJoin.png".format(y_axis), p,  width=10, height=5)
'''
data = []
PlotSelect("index_join_pkfk")
index_join = data

## index join plots: 
## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: cardinality
for y_axis in y_axis_list:
    p = ggplot(index_join, aes(x='g', y=y_axis, color=group_label, fill=group_label, group=group_label, shape=group_label))
    p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)
    p += axis_labels('Cardinality/Selectivity', "{}".format(y_axis), "discrete") + coord_flip() 
    p += facet_wrap("~p~cardinality", scales=esc("free_x"))
    ggsave("micro_{}_indexJoin.png".format(y_axis), p,  width=8, height=6)

