import numpy as np
import pandas as pd
from pygg import *

# for each query, 
def overhead(base, extra):
    return (extra-base)*100
    return max(((extra-base)/base)*100, 0)

    
lcopy = "feb27_copy"
lfull = "feb27_full"

df_data = pd.read_csv("eval_results/micro_benchmark_notes_feb27_logical.csv")
df_data["notes"] = "logical"
temp = pd.read_csv("eval_results/micro_benchmark_notes_feb27_SD.csv")
df_stats = temp[temp["notes"]=="feb27_stats"]#pd.read_csv("eval_results/micro_benchmark_notes_feb26b_stats.csv")
temp = temp[temp["notes"]!="feb27_stats"]
df_data = df_data.append(temp)
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
    #df = df[df['lineage_type'] != "Perm"]
    df_Baseline = df[df["lineage_type"]=="Baseline"]
    df_withB = pd.merge(df, df_Baseline, how='inner', on = ['cardinality', "groups"])
    df_withB["roverhead"] = df_withB.apply(lambda x: overhead(x['runtime_y'], x['runtime_x']), axis=1)
    df_withB= df_withB[df_withB["lineage_type_x"]!="Baseline"]
    

    # full = copy + capture
    # noCapture = copy
    # capture = full - copy
    #print(df_withB.groupby(['cardinality', 'groups', 'lineage_type_x', 'notes_x']).mean())
    df_copy = df_withB[df_withB['notes_x'] == lcopy]
    df_full = df_withB[df_withB['notes_x'] == lfull]
    df_fc = pd.merge(df_copy, df_full, how='inner', on = ['cardinality', "groups"])
    df_fc = df_fc.rename({'roverhead_y': 'full', 'roverhead_x': 'copy', 'output_y_x': 'output'}, axis=1)
    df_fc = df_fc[["full", "copy", "output", "cardinality", "groups"]]
    df_fc["capture"] = df_fc.apply(lambda x: x['full']- x['copy'], axis=1)
    #print(df_fc.groupby(['cardinality', 'groups']).mean())
    
    def normalize(full, nchunks):
        #full *= 100
        if nchunks == 0:
            return full
        else:
            return full/nchunks
    df_fcstats = pd.merge(df_fc, df_stats, how='inner', on = ['cardinality', "groups"])
    df_fcstats["full_overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['full'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["capture_overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['capture'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["copy_overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['copy'],float(x['stats'].split(',')[1])), axis=1)
    df_fcstats["size"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[0])/(1024.0*1024.0), axis=1)
    df_fcstats = df_fcstats[["stats", "size", "output", "full_overhead_nor", "capture_overhead_nor", "copy_overhead_nor", "cardinality", "groups"]]
    
    perm_overhead = df_withB[df_withB["lineage_type_x"]=="Perm"]["roverhead"].aggregate(["mean", "min", "max"])
    sd_overhead = df_fc["full"].aggregate(["mean", "min", "max"])
    print("Perm: ", perm_overhead)
    print("SD: ", sd_overhead)
    print("Speedup: ", perm_overhead / sd_overhead)

    keys = ["full_overhead_nor", "capture_overhead_nor", "copy_overhead_nor", "size"]
    for k in keys:
        # compute speedup
        summary = df_fcstats[k].aggregate(['mean', 'min','max'])
        #print(k, "--->", summary)
    summary = df_fcstats.groupby(["cardinality"])["full_overhead_nor"].aggregate(['mean', 'min','max'])
    #print(k, "--->", summary)
    df_fc = pd.merge(df_fc, df_fcstats, how='inner', on = ['cardinality', "groups"])
    for index, row in df_fc.iterrows():
        vals = ["full", "copy", "capture"]
        for v in vals:
            data.append(dict(system="SD", g=row["groups"], nor=row[v+"_overhead_nor"], card=row["cardinality"], ltype=v, overhead=row[v], gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
            #data.append(dict(system="SD", nchunks=row['stats'].split(',')[1], g=row["groups"], card=row["cardinality"], ltype=v, overhead=10, gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
    
    for index, row in df_withB.iterrows():
        if (row["lineage_type_x"] == "Perm"):
            data.append(dict(system="Perm", ltype="Perm", overhead=int(row["roverhead"]), gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
    
    

alldata = []
data = []
PlotSelect("perfect_agg")
PlotSelect("reg_agg")

p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~gcard", scales=esc("free_y"))
ggsave("micro_overhead_gb.png", p,  width=10, height=10)
alldata.extend(data)

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
PlotSelect("index_join_12MFalse")
PlotSelect("index_join_12MTrue")
PlotSelect("hash_join_pkfk")
PlotSelect("hash_join_mtm")
p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~gcard", scales=esc("free_y"))
ggsave("micro_overhead_hashjoins.png", p,  width=15, height=10)

alldata.extend(data)


# what kind of graph do I want?
# mean of capture and copy and full per operator
# x-axis: operator; y-axis: normalized capture overhead
k = "ltype"
p = ggplot(alldata, aes(x='optype', y='nor', color=k, fill=k, group=k))
p += geom_point(stat=esc('summary'), fun=esc('mean'), alpha=0.8) + coord_flip()
p += axis_labels('Physical Operator', "Relative Overhead per output chunk (%)", "discrete")#, "log10")
ggsave("normalized_overhead.png", p,  width=20, height=10)

"""
k = "card"
p = ggplot(alldata, aes(x='nchunks', y='overhead', color=k, fill=k, group=k))
p += geom_point(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~ltype~optype", scales=esc("free_y"))
ggsave("micro.png", p,  width=10, height=10)
"""
