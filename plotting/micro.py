import numpy as np
import pandas as pd
from pygg import *

# for each query, 
def overhead(base, extra):
    return max(((extra-base)/base)*100, 0)

    
lcopy = "feb26b_copy"
lfull = "feb26b_full"

df_data = pd.read_csv("eval_results/micro_benchmark_notes_feb26b_logical.csv")
df_data["notes"] = "logical"
temp = pd.read_csv("eval_results/micro_benchmark_notes_feb26b_SD.csv")
df_data = df_data.append(temp)
df_stats = pd.read_csv("eval_results/micro_benchmark_notes_feb26b_stats.csv")
pd.set_option("display.max_rows", None)


df_stats = df_stats.drop(columns=["notes", "output", "runtime"])

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
    df = df[df['lineage_type'] != "Perm"]
    df_Baseline = df[df["lineage_type"]=="Baseline"]
    df_withB = pd.merge(df, df_Baseline, how='inner', on = ['cardinality', "groups"])
    df_withB["roverhead"] = df_withB.apply(lambda x: overhead(x['runtime_y'], x['runtime_x']), axis=1)
    df_withB= df_withB[df_withB["lineage_type_x"]!="Baseline"]
    for index, row in df_withB.iterrows():
        if (row["lineage_type_x"] == "Perm"):
            data.append(dict(system="Perm", ltype="Perm", overhead=int(row["roverhead"]), gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))

    # full = copy + capture
    # noCapture = copy
    # capture = full - copy
    #print(df_withB.groupby(['cardinality', 'groups', 'lineage_type_x', 'notes_x']).mean())
    df_copy = df_withB[df_withB['notes_x'] == lcopy]
    df_full = df_withB[df_withB['notes_x'] == lfull]
    df_fc = pd.merge(df_copy, df_full, how='inner', on = ['cardinality', "groups"])
    df_fc = df_fc.drop(columns=["lineage_type_x_x", "lineage_type_y_y", "lineage_type_x_y", "lineage_type_y_x"])
    df_fc = df_fc.rename({'roverhead_y': 'full', 'roverhead_x': 'copy', 'output_y_x': 'output'}, axis=1)
    df_fc["capture"] = df_fc.apply(lambda x: x['full']- x['copy'], axis=1)
    print(df_fc.groupby(['cardinality', 'groups']).mean())
    
    def normalize(full, nchunks):
        if nchunks == 0:
            return full
        else:
            return full/nchunks
    df_fcstats = pd.merge(df_fc, df_stats, how='inner', on = ['cardinality', "groups"])
    df_fcstats["overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['full'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats = df_fcstats.drop(columns=["output_y_y", "runtime_y_y", "runtime_y_x", "runtime_x_x", "runtime_x_y", "output_x_y", "output_x_x"])
    #print(df_fcstats.groupby(['cardinality', 'groups']).mean())
    print("Perm ---> ", df_withB.groupby(["lineage_type_x"])["roverhead"].aggregate(["mean", "min", "max"]))
    keys = ["copy", "full", "capture", "overhead_nor"]
    for k in keys:
        summary = df_fcstats.groupby(['lineage_type'])[k].aggregate(['mean', 'min','max'])
        print(k, "--->", summary)
    summary = df_fcstats.groupby(['lineage_type', "cardinality"])["overhead_nor"].aggregate(['mean', 'min','max'])
    print(k, "--->", summary)
    #print(df_fcstats)
    for index, row in df_fc.iterrows():
        vals = ["full", "copy", "capture"]
        for v in vals:
            data.append(dict(system="SD", g=row["groups"], card=row["cardinality"], ltype=v, overhead=row[v], gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
            #data.append(dict(system="SD", nchunks=row['stats'].split(',')[1], g=row["groups"], card=row["cardinality"], ltype=v, overhead=10, gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
            print(data[len(data)-1])
    
    

alldata = []
data = []
PlotSelect("perfect_agg")
PlotSelect("reg_agg")

p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~gcard", scales=esc("free_y"))
ggsave("micro_overhead_gb.png", p,  width=10, height=10)

data = []
alldata.extend(data)
PlotSelect("scan")
PlotSelect("orderby")
PlotSelect("filter")
PlotSelect("filter_scan")
p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~gcard", scales=esc("free_y"))
ggsave("micro_overhead_scans.png", p,  width=10, height=10)
alldata.extend(data)

data = []
PlotSelect("join_lessthannl")
PlotSelect("join_lessthanmerge")
PlotSelect("bnl_join")
PlotSelect("cross_product")
alldata.extend(data)

p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~gcard", scales=esc("free_y"))
ggsave("micro_overhead_njoins.png", p,  width=10, height=10)

data = []
PlotSelect("index_join_pkfkFalse")
PlotSelect("index_join_pkfkTrue")
PlotSelect("index_join_mtmFalse")
PlotSelect("index_join_mtmTrue")
PlotSelect("hash_join_pkfk")
PlotSelect("hash_join_mtm")
p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~gcard", scales=esc("free_y"))
ggsave("micro_overhead_hashjoins.png", p,  width=15, height=10)

alldata.extend(data)

"""
k = "card"
p = ggplot(alldata, aes(x='nchunks', y='overhead', color=k, fill=k, group=k))
p += geom_point(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~ltype~optype", scales=esc("free_y"))
ggsave("micro.png", p,  width=10, height=10)
"""
