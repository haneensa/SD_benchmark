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

})
legend_none = legend + theme(**{"legend.position": esc("none")})

legend_side = legend + theme(**{
  "legend.position":esc("right"),
})

# for each query, 
def relative_overhead(base, extra): # in %
    return max(((float(extra)-float(base))/float(base))*100, 0)

def overhead(base, extra): # in ms
    return max(((float(extra)-float(base)))*1000, 0)

df = pd.read_csv("eval_results/tpch_benchmark_capture_m18.csv")
df = df[df["lineage_type"]!="Logical-RID"]
df_logical = pd.read_csv("eval_results/tpch_benchmark_capture_a9.csv")

df_logical_opt = df_logical[df_logical['lineage_type']== "Logical-OPT"]
df_logical_rid = df_logical[df_logical['lineage_type']== "Logical-RID"]
#print(df_logical_rid["lineage_type"])
df_logical_rid["lineage_type"] = df_logical_rid.apply(lambda x: x["lineage_type"] if x["query"] in df_logical_opt["query"].values else "Logical-OPT", axis=1)
#print(df_logical_rid["lineage_type"])

# whenever opt does not exist for a query, use the same value as rid
df_logical_missing = df_logical_rid[df_logical_rid["lineage_type"]=="Logical-OPT"]
df_logical = df_logical.append(df_logical_missing)
df = df.append(df_logical)
df_stats = df[df["notes"]=="m18_stats"]
df = df[df["notes"]!="m18_stats"]
#df = df[df["sf"]==1]
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
df_stats = df_stats[["stats", "query", "sf", 'n_threads']]

def normalize(full, nchunks):
    #full *= 100
    if nchunks == 0:
        return full
    else:
        return full/nchunks
df_fcstats = pd.merge(df_fc, df_stats, how='inner', on = ['query', 'sf', 'n_threads'])
df_fcstats["full_overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_x'],float(x['stats'].split(',')[1])), axis=1)
df_fcstats["copy_overhead_nor"] = df_fcstats.apply(lambda x: normalize(x['overhead_x'],float(x['stats'].split(',')[1])), axis=1)
df_fcstats["nchunks"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[1]), axis=1)
df_fcstats["size"] = df_fcstats.apply(lambda x: float(x['stats'].split(',')[0])/(1024*1024), axis=1)
df_fcstats = df_fcstats.drop(columns=["stats"])
print(df_fcstats) 


print("Logical vs Full")
df_fc = pd.merge(df_fcstats, df_logical, how='inner', on = ['query', "sf", 'n_threads'])
print(df_fc) 
print(df_fc[["sf", "n_threads", "query", "rel_overhead_y", "rel_overhead", "overhead_y", "overhead"]].groupby(["sf", 'n_threads']).aggregate(["mean", "min", "max"]))

type1 = ['1', '3', '5', '6', '7', '8', '9', '10', '12', '13', '14', '19']

type2 = ['11', '15', '16', '18']
type3 = ['2', '4', '17', '20', '21', '22']

def cat(qid):
    if qid in type1:
        return "1. Joins-Aggregations"
    elif qid in type2:
        return "2. Uncorrelated subQs"
    else:
        return "3. Correlated subQs"
df_withB["qtype"] = df_withB.apply(lambda x: cat(str(x['query'])), axis=1)
class_list = type1
class_list.extend(type2)
class_list.extend(type3)
queries_order = [""+str(x)+"" for x in class_list]
queries_order = ','.join(queries_order)

print(df_fcstats[df_fcstats['query'].isin(type3)].groupby(["query", "sf", 'n_threads']).mean())
print(df_fcstats[df_fcstats['query'].isin(type3)].groupby(["sf", 'n_threads']).mean())

print(df_fcstats[["sf", "n_threads", "query", "nchunks", "size"]].groupby(["sf", 'n_threads']).aggregate(["mean", "min", "max"]))
for index, row in df_withB.iterrows():
    if row["sf"] != 10:
        continue
    if (row["notes"] == "m18_copy"):
        continue
    name = row['lineage_type']# + row["notes"]
    if name == "Logical-RID":
        name = "Logical"
    elif name == "SD_Capture":
        name = "SD"

    sf = "'{}'".format(str(row["sf"]))
    qtype = row['qtype']
    notes = row['notes']
    over = row['overhead']
    rel_over = row['rel_overhead']
    qid = str(row['query'])
    data.append(dict(sf=sf, qtype=qtype, system=name, notes=notes, overhead=over, rel_overhead=rel_over, qid=qid))

sd_data = []
for index, row in df_fcstats.iterrows():
    if row["sf"] == 20:
        continue
    name = "SD"
    qid = str(row['query'])
    sf = "'{}'".format(str(row["sf"]))
    size = row["size"]
    nchunks = row["nchunks"]
    sd_data.append(dict(qid=qid, size=size, stype="MB", sf=sf))
    #sd_data.append(dict(qid=qid, size=nchunks, stype="#logs", sf=sf))

y_axis_list = ["rel_overhead", "overhead"]
header = ["Relative \nOverhead %", "Overhead (ms)"]
for idx, y_axis in enumerate(y_axis_list):
    p = ggplot(data, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='system'))
    #p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5, size=2)
    p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 1000, 100000], labels=list(map(esc, ['10', '1000', '100k']))))
    p += legend_bottom
    p += legend_side
    p += facet_grid(".~qtype", scales=esc("free_x"), space=esc("free_x"))
    postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
    ggsave("tpch_{}.png".format(y_axis), p, postfix=postfix,  width=12, height=2.25, scale=0.8)




sd_data = []
print(df_fcstats[["sf", "n_threads", "query", "nchunks", "size"]].groupby(["sf", 'n_threads']).aggregate(["mean", "min", "max"]))
for index, row in df_withB.iterrows():
    if (row["notes"] == "m18_copy"):
        continue
    if row['lineage_type'] != "SD_Capture": continue
    name = "SD"
    sf = int(row['sf'])
    qtype = row['qtype']
    notes = row['notes']
    over = row['overhead']
    rel_over = row['rel_overhead']
    qid = str(row['query'])
    sd_data.append(dict(sf=sf, qtype=qtype, system=name, notes=notes, overhead=over, rel_overhead=rel_over, qid=qid))




p = ggplot(sd_data, aes(x='sf', ymin=0, ymax='rel_overhead',  y='rel_overhead', color='qtype',  group='qid', shape='qtype'))
#p += geom_smooth()
p += geom_point() + geom_line()
p += axis_labels('Scale Factor', 'Relative\nOverhead (log)', "continuous", "continuous")#, ykwargs=dict(breaks=[10, 1000, 100000], labels=list(map(esc, ['10', '1000', '100k']))))
p += legend_none
p += facet_grid(".~qtype", scales=esc("free_x"), space=esc("free_x"))
postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
ggsave("tpch_reloverhead_sd_varysf.png".format(y_axis), p, postfix=postfix,  width=6, height=2.25, scale=0.8)



if 0:
    queries_order = [""+str(x)+"" for x in range(1,23)]
    queries_order = ','.join(queries_order)
    p = ggplot(sd_data, aes(x='qid', y="size", fill="sf", group="sf"))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', "Size (MB) [log]", "discrete", "log10") + coord_flip()
    p += legend_bottom
    postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
    ggsave("tpch_metrics.png", p, postfix=postfix,  width=2.5, height=4)
