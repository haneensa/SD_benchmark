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
def overhead(base, extra):
    return max(((extra-base)/base)*100, 0)

df = pd.read_csv("eval_results/tpch_benchmark_capture.csv")
pd.set_option("display.max_rows", None)

data = []

df_Baseline = df[df["lineage_type"]=="Baseline"]
df_withB = pd.merge(df, df_Baseline, how='inner', on = ['query', 'sf', 'n_threads'])
df_withB["roverhead"] = df_withB.apply(lambda x: overhead(x['runtime_y'], x['runtime_x']), axis=1)
df_withB= df_withB[df_withB["lineage_type_x"]!="Baseline"]
df_withB= df_withB[df_withB["notes_x"]!="SDv1"]
for index, row in df_withB.iterrows():
    #data.append(dict(system=row['lineage_type_x']+row["notes_x"], overhead=int(row["roverhead"]), qid=row['query']))
    data.append(dict(system=row['lineage_type_x'], overhead=int(row["roverhead"]), qid=row['query']))

p = ggplot(data, aes(x='qid', y='overhead', color='system', fill='system', group='system', shape='system'))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Relative Overhead % (log)", "discrete", "log10")
#p += ylim(lim=[0,300])
p += legend_bottom
ggsave("tpch_overhead.png", p,  width=6, height=3)
