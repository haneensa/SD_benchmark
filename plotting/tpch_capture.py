import pandas as pd
from pygg import *
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
    return max(((extra-base)/base)*100, 0)

def overhead(base, extra): # in ms
    return max(((extra-base))*1000, 0)

df = pd.read_csv("eval_results/tpch_benchmark_capture_feb28.csv")
df = df[df["sf"] == 1]
pd.set_option("display.max_rows", None)

data = []

df = df.drop(columns=["stats", "repeat"])
df_Baseline = df[df["lineage_type"]=="Baseline"]
df_Baseline = df_Baseline[["runtime", "query", "sf", "n_threads", "output"]]
df_withB = pd.merge(df, df_Baseline, how='inner', on = ['query', 'sf', 'n_threads'])
df_withB["rel_overhead"] = df_withB.apply(lambda x: relative_overhead(x['runtime_y'], x['runtime_x']), axis=1)
df_withB["overhead"] = df_withB.apply(lambda x: overhead(x['runtime_y'], x['runtime_x']), axis=1)
df_withB["outputFan"] = df_withB.apply(lambda x: x['output_x']/ x['output_y'], axis=1)
df_withB= df_withB[df_withB["lineage_type"]!="Baseline"]
print(df_withB)
df_full = df_withB[df_withB["notes"] == "feb28_full"]
df_copy = df_withB[df_withB["notes"] == "feb28_copy"]
df_fc = pd.merge(df_copy, df_full, how='inner', on = ['query', "sf", 'n_threads'])
for index, row in df_withB.iterrows():
    name = row['lineage_type'] + row["notes"]
    data.append(dict(system=name, overhead=row["overhead"], rel_overhead=row["rel_overhead"], qid=row['query']))

p = ggplot(data, aes(x='qid', y='overhead', color='system', fill='system', group='system', shape='system'))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Runtime Overhead (ms)", "discrete", "log10")
#p += ylim(lim=[0,300])
p += legend_bottom
ggsave("tpch_overhead.png", p,  width=6, height=3)

p = ggplot(data, aes(x='qid', y='rel_overhead', color='system', fill='system', group='system', shape='system'))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Runtime Relative Overhead % (log)", "discrete", "log10")
#p += ylim(lim=[0,300])
p += legend_bottom
ggsave("tpch_relative_overhead.png", p,  width=6, height=3)
