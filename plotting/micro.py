import pandas as pd
from pygg import *

# for each query, 
def overhead(base, extra):
    return max(((extra-base)/base)*100, 0)

df_data = pd.read_csv("eval_results/micro_benchmark_notes_feb18.csv")
df_data["experiment"] = "baseline"
df_data2 = pd.read_csv("eval_results/micro_benchmark_notes_feb18_SD_captureAndCopy.csv")
df_data2["experiment"] = "captureAndCopy"
df_data3 = pd.read_csv("eval_results/micro_benchmark_notes_feb18_SD_Copy.csv")
df_data3["experiment"] = "Copy"
df_data = df_data.append(df_data2)
df_data = df_data.append(df_data3)


# 1. Partition by query time [orderby, filter, filter_scan, perfect_agg, reg_agg, groupby
#   a. Order by: vary cardinality (1M, 5M, 10M)
#   b. Filter & Filter Scan: vary cardinality (1M, 5M, 10M), vary selectivity: 0, 0.2, ,0.5, 1.0
#   c. aggs: vary cardinality, vary number of groups
#   d. merge, nlj, bnlj, cross product: vary cardinality of two tables
#   e. hash_join pkfk
#   f. index_join pkfk: 

def PlotSelect(filterType):
    df_filter = df_data[df_data['query'] == filterType]
    df_filter_Baseline = df_filter[df_filter["lineage_type"]=="Baseline"]
    df_filter = df_filter.drop(columns=["query"])
    df_filter = pd.merge(df_filter, df_filter_Baseline, how='inner', on = ['cardinality', "groups"])
    df_filter["reloative_overead"] = df_filter.apply(lambda x: overhead(x['runtime_y'], x['runtime_x']), axis=1)
    print(df_filter.groupby(['cardinality', 'groups', 'lineage_type_x', 'experiment_x']).mean())

#PlotSelect("orderby")
#PlotSelect("filter")
PlotSelect("filter_scan")
#PlotSelect("perfect_agg")
#PlotSelect("reg_agg")
#PlotSelect("join_lessthannl")
#PlotSelect("join_lessthanmerge")
#PlotSelect("bnl_join")
#PlotSelect("cross_product")
#PlotSelect("hash_join_pkfk")
#PlotSelect("hash_join_mtm")
#PlotSelect("index_join_pkfk")
#PlotSelect("index_join_mtm")





    
