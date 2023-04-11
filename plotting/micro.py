import numpy as np
import pandas as pd
from pygg import *

# for each query, 
def overhead(base, extra):
    return (extra-base)*1000

def relative_overhead(base, extra):
    return max(((extra-base)/base)*100, 0)

def fanout(a, b):
    if b == 0: return 0
    return a / b
    
lcopy = "m15_copy"
lfull = "m15_full"

df_all = pd.read_csv("eval_results/micro_benchmark_notes_m15.csv")
df_logical = df_all[df_all["notes"]=="m15"]
df_logical["notes"] = "logical"
df_stats = df_all[df_all["notes"]=="m15_stats"]#pd.read_csv("eval_results/micro_benchmark_notes_feb26b_stats.csv")
df_full = df_all[df_all["notes"]=="m15_full"]
df_copy = df_all[df_all["notes"]=="m15_copy"]

df_data = df_logical
df_data = df_data.append(df_full)
df_data = df_data.append(df_copy)

pd.set_option("display.max_rows", None)



# 1. Partition by query time [orderby, filter, filter_scan, perfect_agg, reg_agg, groupby
#   a. Order by: vary cardinality (1M, 5M, 10M)
#   b. Filter & Filter Scan: vary cardinality (1M, 5M, 10M), vary selectivity: 0, 0.2, ,0.5, 1.0
#   c. aggs: vary cardinality, vary number of groups
#   d. merge, nlj, bnlj, cross product: vary cardinality of two tables
#   e. hash_join pkfk
#   f. index_join pkfk: 

def PlotSelect(filterType):
    print("****************** Summary for : ", filterType)
    df = df_data[df_data['query'] == filterType]
    df = df.drop(columns=["query", "stats"])
    df_Baseline = df[df["lineage_type"]=="Baseline"]
    df_Baseline = df_Baseline[["cardinality", "groups", "runtime", "output"]]
    df_Baseline = df_Baseline.rename({'runtime':"Bruntime", 'output': 'Boutput'}, axis=1)
    df= df[df["lineage_type"]!="Baseline"]

    df_withB = pd.merge(df, df_Baseline, how='inner', on = ['cardinality', "groups"])
    df_withB["roverhead"] = df_withB.apply(lambda x: relative_overhead(x['Bruntime'], x['runtime']), axis=1)
    df_withB["overhead"] = df_withB.apply(lambda x: overhead(x['Bruntime'], x['runtime']), axis=1)
    df_withB["fanout"] = df_withB.apply(lambda x: fanout(x['output'],float(x['Boutput'])), axis=1)

    # full = copy + capture
    # noCapture = copy
    # capture = full - copy
    #print(df_withB.groupby(['cardinality', 'groups', 'lineage_type_x', 'notes_x']).mean())
    df_copy = df_withB[df_withB['notes'] == lcopy]
    df_full = df_withB[df_withB['notes'] == lfull]
    
    df_full = df_full.drop(columns=["lineage_type", "output", "fanout"])
    
    df_full = df_full[["roverhead", "overhead", "runtime", "cardinality", "groups"]]
    df_copy = df_copy[["roverhead", "overhead", "runtime", "cardinality", "groups"]]
    
    df_full = df_full.rename({'roverhead': 'roverhead_f','overhead':'overhead_f', 'runtime':"runtime_f" }, axis=1)
    df_copy = df_copy.rename({'roverhead': 'roverhead_c','overhead':'overhead_c', 'runtime':"runtime_c" }, axis=1)
    
    df_fc = pd.merge(df_copy, df_full, how='inner', on = ['cardinality', "groups"])

    def normalize(full, nchunks):
        #full *= 100
        if nchunks == 0:
            return full
        else:
            return full/nchunks
    
    df_statsq = df_stats[df_stats['query'] == filterType]
    df_statsq = df_statsq[["stats", "cardinality", "groups"]]#.drop(columns=["notes", "output", "runtime"])
    df_fcstats = pd.merge(df_fc, df_statsq, how='inner', on = ['cardinality', "groups"])

    df_fcstats["foverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_f'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["coverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_c'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["froverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['roverhead_f'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["croverhead_nor"] = df_fcstats.apply(lambda x: normalize(x['roverhead_c'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["size"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[0])/(1024.0*1024.0), axis=1)
    df_fcstats["nchunks"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[1]), axis=1)
    df_fcstats = df_fcstats.drop(columns=["stats"])
    
    perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].aggregate(["mean", "min", "max"])
    print("Perm: ", perm_overhead[ ["overhead", "fanout", "roverhead", "runtime", "Bruntime"] ]) 
    sd_overhead = df_fcstats.aggregate(["mean", "min", "max"])
    print("SD: ", sd_overhead[['overhead_f', 'runtime_f', 'roverhead_f', 'foverhead_nor']])
    print("Speedup: ", perm_overhead['runtime'] / sd_overhead['runtime_f'])

    ############3
    perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].groupby(["cardinality","groups"]).aggregate(["mean"])
    print("Perm: ", perm_overhead[ ["overhead", "fanout", "roverhead", "runtime", "Bruntime"] ]) 
    sd_overhead = df_fcstats.groupby(["cardinality", "groups"]).aggregate(["mean"])
    print("SD: ", sd_overhead[['overhead_f', 'runtime_f', 'roverhead_f', 'foverhead_nor']])
    print("Speedup: ", perm_overhead['runtime'] / sd_overhead['runtime_f'])

    for index, row in df_fcstats.iterrows():
        vals = ['f']#, 'c']
        if (row["cardinality"] == 1000): row["cardinality"] = "1K"
        if (row["cardinality"] == 10000): row["cardinality"] = "10K"
        if (row["cardinality"] == 100000): row["cardinality"] = "100K"
        if (row["cardinality"] == 1000000): row["cardinality"] = "1M"
        if (row["cardinality"] == 5000000): row["cardinality"] = "5M"
        if (row["cardinality"] == 10000000): row["cardinality"] = "10M"

        for v in vals:
            data.append(dict(g2="SD/"+filterType, system="SD", g1="{}/{}".format(row["cardinality"],str(row["groups"])), g=row["groups"], card=row["cardinality"], ltype=v, roverhead=row['roverhead_'+v], overhead=row['overhead_'+v], optype=filterType))
    
    for index, row in df_withB.iterrows():
        if (row["cardinality"] == 1000): row["cardinality"] = "1K"
        if (row["cardinality"] == 10000): row["cardinality"] = "10K"
        if (row["cardinality"] == 100000): row["cardinality"] = "100K"
        if (row["cardinality"] == 1000000): row["cardinality"] = "1M"
        if (row["cardinality"] == 5000000): row["cardinality"] = "5M"
        if (row["cardinality"] == 10000000): row["cardinality"] = "10M"
        if (row["lineage_type"] == "Perm"):
            data.append(dict(g2="Perm/"+filterType, system="Perm", g1=row["cardinality"]+"/"+str(row["groups"]), g=row["groups"], card=row["cardinality"], ltype="Perm", roverhead=row['roverhead'], overhead=row["overhead"],  optype=filterType))
    

alldata = []
data = []
group_label = "g2"
"""
PlotSelect("perfect_agg")
PlotSelect("reg_agg")

p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~card~g", scales=esc("free_y"))
ggsave("micro_overhead_gb.png", p,  width=10, height=10)
alldata.extend(data)

data = []
alldata.extend(data)
PlotSelect("scan")
PlotSelect("orderby")
PlotSelect("filter")
PlotSelect("filter_scan")

## filter plots: 
## 1) X-axis: cardinality, Y-axis: relative overhead; group: selectivity+optype
cardinality_str = ["1M", "5M", "10M"]
selections_str = ["0.0", "0.2", "0.5", "1.0"]
group_order= ["'{}/{}'".format(a, b) for a in cardinality_str for b in selections_str]
group_order = ','.join(group_order)

postfix = "data$card= factor(data$card, levels=c({}))".format(group_order)
p = ggplot(data, aes(x='g1', y='roverhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('Cardinality/Selectivity', "Relative Overhead (%)", "discrete")#, "log10")
ggsave("micro_roverhead_filter.png", p,  postfix=postfix, width=10, height=4)

p = ggplot(data, aes(x='g1', y='overhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('Cardinality/Selectivity', "Overhead (ms)", "discrete")#, "log10")
ggsave("micro_overhead_filter.png", p,  postfix=postfix, width=10, height=4)

## 2) detailed
p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~card~g", scales=esc("free_y"))
ggsave("micro_overhead_scans.png", p,  width=15, height=10)
alldata.extend(data)
"""

data = []
sels = [0.2, 0.8, 0.5, 0.0]
for sel in sels:
    PlotSelect("nl_{}".format(sel))
    PlotSelect("merge_{}".format(sel))
    PlotSelect("bnl_{}".format(sel))
alldata.extend(data)
p = ggplot(data, aes(x='g1', y='roverhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('Cardinality/Selectivity', "Relative Overhead (%)", "discrete")#, "log10")
ggsave("micro_roverhead_none.png", p,   width=10, height=4)

p = ggplot(data, aes(x='g1', y='overhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('Cardinality/Selectivity', "Overhead (ms)", "discrete")#, "log10")
ggsave("micro_overhead_none.png", p,  width=10, height=4)

#PlotSelect("cross_0.0")

"""

data = []
PlotSelect("index_join_pkfk_a1_False")
PlotSelect("index_join_pkfk_a0_False")
PlotSelect("index_join_pkfk_a1_True")
PlotSelect("index_join_pkfk_a0_True")

alldata.extend(data)
p = ggplot(data, aes(x='g1', y='roverhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Relative Overhead (%)", "discrete")#, "log10")
ggsave("micro_roverhead_indexjoin_pkfk.png", p, width=10, height=4)

p = ggplot(data, aes(x='g1', y='overhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Overhead (ms)", "discrete")#, "log10")
ggsave("micro_overhead_indexjoin_pkfk.png", p, width=10, height=4)

data = []
PlotSelect("hash_join_pkfk0")
PlotSelect("hash_join_pkfk1")

alldata.extend(data)
p = ggplot(data, aes(x='g1', y='roverhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Relative Overhead (%)", "discrete")#, "log10")
ggsave("micro_roverhead_hashjoin_pkfk.png", p,  postfix=postfix, width=10, height=4)

p = ggplot(data, aes(x='g1', y='overhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Overhead (ms)", "discrete")#, "log10")
ggsave("micro_overhead_hashjoin_pkfk.png", p,  postfix=postfix, width=10, height=4)


data = []
PlotSelect("index_join_mtmFalse")
PlotSelect("index_join_mtmTrue")
p = ggplot(data, aes(x='g1', y='roverhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Relative Overhead (%)", "discrete")#, "log10")
ggsave("micro_roverhead_indexjoin_mtm.png", p,  postfix=postfix, width=10, height=4)

p = ggplot(data, aes(x='g1', y='overhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Overhead (ms)", "discrete")#, "log10")
ggsave("micro_overhead_indexjoin_mtm.png", p,  postfix=postfix, width=10, height=4)



data = []
PlotSelect("hash_join_mtm")
alldata.extend(data)

p = ggplot(data, aes(x='g1', y='roverhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Relative Overhead (%)", "discrete")#, "log10")
ggsave("micro_roverhead_hashjoin_pkfk.png", p,  postfix=postfix, width=10, height=4)

p = ggplot(data, aes(x='g1', y='overhead', color=group_label, fill=group_label, group=group_label, shape=group_label))
p += geom_bar(stat=esc('identity'), position=esc("dodge"), alpha=0.8, width=0.5)# + coord_flip()
p += axis_labels('n/g', "Overhead (ms)", "discrete")#, "log10")
ggsave("micro_overhead_hashjoin_mtm.png", p,  postfix=postfix, width=10, height=4)

# what kind of graph do I want?
# mean of capture and copy and full per operator
# x-axis: operator; y-axis: normalized capture overhead
k = "ltype"
p = ggplot(alldata, aes(x='optype', y='nor', color=k, fill=k, group=k))
p += geom_point(stat=esc('summary'), fun=esc('mean'), alpha=0.8) + coord_flip()
p += axis_labels('Physical Operator', "Relative Overhead per output chunk (%)", "discrete")#, "log10")
ggsave("normalized_overhead.png", p,  width=20, height=10)

k = "card"
p = ggplot(alldata, aes(x='nchunks', y='overhead', color=k, fill=k, group=k))
p += geom_point(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~ltype~optype", scales=esc("free_y"))
ggsave("micro.png", p,  width=10, height=10)
"""
