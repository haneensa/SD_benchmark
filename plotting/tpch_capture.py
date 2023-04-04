import pandas as pd
from pygg import *

class_list = [1,3,5,6,7,8,9,10,12,13,14,19, 2,4,11,15,16, 17,18,20,21,22]
queries_order = [""+str(x)+"" for x in class_list]
queries_order = ','.join(queries_order)


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

}),
# for each query, 
def relative_overhead(base, extra): # in %
    return max(((float(extra)-float(base))/float(base))*100, 0)

def overhead(base, extra): # in ms
    return max(((float(extra)-float(base)))*1000, 0)

df = pd.read_csv("eval_results/tpch_benchmark_capture_m18.csv")
df_stats = df[df["notes"]=="m18_stats"]
df = df[df["notes"]!="m18_stats"]
df = df[df["sf"]==1]
pd.set_option("display.max_rows", None)
#pd.set_option("display.max_columns", None)

data = []

df = df.drop(columns=["stats", "repeat"])
df_Baseline = df[df["lineage_type"]=="Baseline"]
df_Baseline = df_Baseline[["runtime", "query", "sf", "n_threads", "output"]]
df_withB = pd.merge(df, df_Baseline, how='inner', on = ['query', 'sf', 'n_threads'])
df_withB["rel_overhead"] = df_withB.apply(lambda x: relative_overhead(x['runtime_y'], x['runtime_x']), axis=1)
df_withB["overhead"] = df_withB.apply(lambda x: overhead(x['runtime_y'], x['runtime_x']), axis=1)
df_withB["outputFan"] = df_withB.apply(lambda x: float(x['output_x'])/ float(x['output_y']), axis=1)
df_withB= df_withB[df_withB["lineage_type"]!="Baseline"]
#print(df_withB)

df_sd = df_withB[df_withB["lineage_type"] == "SD_Capture"]
df_full = df_sd[df_sd["notes"] == "m18"]
df_copy = df_sd[df_sd["notes"] == "m18_copy"]
df_logical = df_withB[df_withB["lineage_type"] == "Logical-RID"]
df_logical = df_logical[["query", "sf", "n_threads", "rel_overhead", "overhead", "outputFan", "runtime_x"]]
df_full = df_full[["query", "sf", "n_threads", "rel_overhead", "notes", "overhead",  "runtime_x", "runtime_y"]]
df_copy = df_copy[["query", "sf", "n_threads", "rel_overhead", "notes", "overhead", "runtime_x"]]
df_fc = pd.merge(df_copy, df_full, how='inner', on = ['query', "sf", 'n_threads'])
print("Copy vs Full")
print(df_fc)
df_fc = pd.merge(df_full, df_logical, how='inner', on = ['query', "sf", 'n_threads'])

def normalize(full, nchunks):
    #full *= 100
    if nchunks == 0:
        return full
    else:
        return full/nchunks

print("Logical vs Full")
df_stats = df_stats[["stats", "query", "sf", 'n_threads']]

df_fcstats = pd.merge(df_fc, df_stats, how='inner', on = ['query', 'sf', 'n_threads'])
df_fcstats["full_overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_x'],float(x['stats'].split(',')[1])), axis=1)
df_fcstats["copy_overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_x'],float(x['stats'].split(',')[1])), axis=1)
df_fcstats["nchunks"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[1]), axis=1)
df_fcstats["size"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[0])/(1024*1024), axis=1)
df_fcstats = df_fcstats.drop(columns=["stats"])
print(df_fcstats) 

type1 = [1, 3, 5, 6, 7, 8, 9, 10, 12, 13, 14, 19]
type2 = [11, 15, 16, 18]
type3 = [2, 4, 17, 18, 20, 21, 22]
print(df_fcstats[df_fcstats['query'].isin(type3)].groupby(["query", "sf", 'n_threads']).mean())
print(df_fcstats[df_fcstats['query'].isin(type3)].groupby(["sf", 'n_threads']).mean())

for index, row in df_withB.iterrows():
    if (row["notes"] == "m18_copy"):
        continue
    name = row['lineage_type']# + row["notes"]
    data.append(dict(system=name, notes=row["notes"], overhead=row["overhead"], rel_overhead=row["rel_overhead"], qid=row['query']))

p = ggplot(data, aes(x='qid', y='overhead', color='system', fill='system', group='system', shape='system'))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Runtime Overhead (ms)", "discrete", "log10")
#p += ylim(lim=[0,300])
p += legend_bottom
postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
ggsave("tpch_overhead.png", p,  width=6, height=3.5)

p = ggplot(data, aes(x='qid', y='rel_overhead', color='system', fill='system', group='system', shape='system'))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Relative Overhead % (log)", "discrete", "log10")
postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
#p += ylim(lim=[0,300])
p += legend_bottom
ggsave("tpch_relative_overhead.png", p, postfix=postfix, width=6, height=3)
