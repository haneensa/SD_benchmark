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
    
lcopy = "m1_copy"
lfull = "m1_full"

df_data = pd.read_csv("eval_results/micro_benchmark_notes_feb27_logical.csv")
df_data["notes"] = "logical"
temp = pd.read_csv("eval_results/micro_benchmark_notes_m1.csv")
df_stats = temp[temp["notes"]=="m1_stats"]#pd.read_csv("eval_results/micro_benchmark_notes_feb26b_stats.csv")
temp = temp[temp["notes"]!="m1_stats"]
df_data = df_data.append(temp)
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
    #df = df[df['lineage_type'] != "Perm"]
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
    df_copy = df_copy.drop(columns=["lineage_type", "fanout", "output", "Boutput", "Bruntime", "fanout"])

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
    df_fcstats = df_fcstats.drop(columns=["stats", "notes_x", "notes_y"])
    
    perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].aggregate(["mean", "min", "max"])
    #perm_overhead = df_withB[df_withB["lineage_type"]=="Perm"].groupby(["cardinality"]).aggregate(["mean", "min", "max"])
    print("Perm: ", perm_overhead[ ["overhead", "fanout", "roverhead", "runtime", "Bruntime"] ]) 
    sd_overhead = df_fcstats.aggregate(["mean", "min", "max"])
    #sd_overhead = df_fcstats.groupby("cardinality").aggregate(["mean", "min", "max"])
    print("SD: ", sd_overhead[['overhead_f', 'overhead_c', 'runtime_c', 'runtime_f', 'roverhead_c', 'roverhead_f', 'coverhead_nor', 'foverhead_nor']])
    print("Speedup: ", perm_overhead['runtime'] / sd_overhead['runtime_f'])

    for index, row in df_fcstats.iterrows():
        vals = ['f', 'c']
        for v in vals:
            data.append(dict(system="SD", g=row["groups"], nor=row[v+"overhead_nor"], card=row["cardinality"], ltype=v, overhead=row['roverhead_'+v], gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
            #data.append(dict(system="SD", nchunks=row['stats'].split(',')[1], g=row["groups"], card=row["cardinality"], ltype=v, overhead=10, gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
    
    for index, row in df_withB.iterrows():
        if (row["lineage_type"] == "Perm"):
            data.append(dict(system="Perm", ltype="Perm", overhead=row["overhead"], gcard=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
    
    

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
ggsave("micro_overhead_scans.png", p,  width=15, height=10)
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
