import pandas as pd
from pygg import *

# for each query, 
def overhead(base, extra):
    return max(((extra-base)/base)*100, 0)

df_data = pd.read_csv("eval_results/micro_benchmark_notes_feb19_baselineAndPerm.csv")
df_data["notes"] = "logical"
df_data2 = pd.read_csv("eval_results/micro_benchmark_notes_feb20_SD.csv")
df_data = df_data.append(df_data2)
df_data = df_data[df_data['notes']!="offset_ldata"]
df_data = df_data[df_data['notes']!="offset_setChildv2"]
df_data = df_data[df_data['notes']!= "offset_setChild"]
pd.set_option("display.max_rows", None)


# 1. Partition by query time [orderby, filter, filter_scan, perfect_agg, reg_agg, groupby
#   a. Order by: vary cardinality (1M, 5M, 10M)
#   b. Filter & Filter Scan: vary cardinality (1M, 5M, 10M), vary selectivity: 0, 0.2, ,0.5, 1.0
#   c. aggs: vary cardinality, vary number of groups
#   d. merge, nlj, bnlj, cross product: vary cardinality of two tables
#   e. hash_join pkfk
#   f. index_join pkfk: 

def PlotSelect(filterType):
    print("Summary for : ", filterType)
    df = df_data[df_data['query'] == filterType]
    df = df.drop(columns=["query", "stats"])
    #df = df[df['lineage_type'] != "Perm"]
    df_Baseline = df[df["lineage_type"]=="Baseline"]
    df_withB = pd.merge(df, df_Baseline, how='inner', on = ['cardinality', "groups"])
    df_withB["roverhead"] = df_withB.apply(lambda x: overhead(x['runtime_y'], x['runtime_x']), axis=1)
    df_withB= df_withB[df_withB["lineage_type_x"]!="Baseline"]
    #for index, row in df_withB.iterrows():
    #    if (row["lineage_type_x"] == "Perm"):
    #        data.append(dict(system="Perm", ltype="Perm", overhead=int(row["roverhead"]), output=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))

    # full = copy + capture
    # noCapture = copy
    # capture = full - copy
    #print(df_withB.groupby(['cardinality', 'groups', 'lineage_type_x', 'notes_x']).mean())
    
    df_noCapture = df_withB[df_withB['notes_x'] == "feb20_noCapture"]
    df_full = df_withB[df_withB['notes_x'] == "feb20_full"]
    df_fc = pd.merge(df_noCapture, df_full, how='inner', on = ['cardinality', "groups"])
    df_fc = df_fc.drop(columns=["lineage_type_x_x", "lineage_type_y_y", "lineage_type_x_y", "lineage_type_y_x"])
    df_fc = df_fc.rename({'roverhead_y': 'full', 'roverhead_x': 'copy', 'output_y_x': 'output'}, axis=1)
    df_fc["capture"] = df_fc.apply(lambda x: x['full']- x['copy'], axis=1)
    print(df_fc.groupby(['cardinality', 'groups']).mean())
    
    print(df_fc)

    for index, row in df_fc.iterrows():
        data.append(dict(system="SD", ltype="full", overhead=int(row["full"]), output=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
        data.append(dict(system="SD", ltype="copy", overhead=int(row["copy"]), output=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
        data.append(dict(system="SD", ltype="capture", overhead=int(row["capture"]), output=str(row["cardinality"])+"~"+str(row["groups"]), optype=filterType))
        print(int(row["full"]))
    
    

data = []
PlotSelect("scan")
PlotSelect("orderby")
PlotSelect("filter")
PlotSelect("filter_scan")
p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~output", scales=esc("free_y"))
ggsave("micro_overhead_scans.png", p,  width=10, height=10)

data = []
PlotSelect("perfect_agg")
PlotSelect("reg_agg")
p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~output", scales=esc("free_y"))
ggsave("micro_overhead_gb.png", p,  width=10, height=10)

data = []
PlotSelect("join_lessthannl")
PlotSelect("join_lessthanmerge")
PlotSelect("bnl_join")
PlotSelect("cross_product")
PlotSelect("index_join_pkfk")
PlotSelect("index_join_mtm")

p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~output", scales=esc("free_y"))
ggsave("micro_overhead_njoins.png", p,  width=10, height=10)


data = []
PlotSelect("hash_join_pkfk")
PlotSelect("hash_join_mtm")
p = ggplot(data, aes(x='ltype', y='overhead', color='ltype', fill='ltype', group='ltype', shape='ltype'))
p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5)# + coord_flip()
p += facet_wrap("~optype~output", scales=esc("free_y"))
ggsave("micro_overhead_hashjoins.png", p,  width=10, height=10)
