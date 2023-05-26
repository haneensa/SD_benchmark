import duckdb
import pandas as pd
from pygg import *
# Source Sans Pro Light
legend = theme_bw() + theme(**{
  "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
  "legend.justification":"c(1,0)", "legend.position":"c(1,0)",
  "legend.key" : element_blank(),
  "legend.title":element_blank(),
  "legend.margin": margin(t = 0, unit=esc('cm')),
  "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
  "axis.text": element_text(colour = "'#333333'", size=11),  
  "plot.background": element_blank(),
  "plot.margin": margin(t = 0, unit=esc('cm')),
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

legend_side = legend + theme(**{
  "legend.position":esc("right"),
})


query_list = [1, 4, 14, 15, 17]
query_list_to_pos_map = {
    q: idx for (idx, q) in enumerate(query_list)
}
no_indexes = pd.read_csv('tpch_sf1_ablation1_2_28_2023.csv')
zone_maps = pd.read_csv('tpch_sf1_ablation3_2_28_2023_2.csv')
smokedduck = pd.read_csv('tpch_sf1_2_28_2023_2.csv')
postprocess = pd.read_csv('postprocess_times3.csv')
no_indexes['ablation'] = 'No Indexes'
zone_maps['ablation'] = '+Zone Maps'
smokedduck['ablation'] = '+Group By Idxs'
postprocess = postprocess[postprocess['ablation'] == -1]
postprocess['ablation'] = 'Postprocess'

duckdb.register("data1", no_indexes)
duckdb.register("data2", zone_maps)
duckdb.register("data3", smokedduck)
duckdb.register("data4", postprocess)
data = duckdb.sql("""
with data as (
           select queryId as query_id, sf, ablation, avg_duration from data1 UNION ALL
           select queryId as query_id, sf, ablation, avg_duration from data2 UNION ALL
           select queryId as query_id, sf, ablation, avg_duration from data3 UNION ALL
           select query_id, sf, ablation, 
            (time_in_sec1 + time_in_sec2 + time_in_sec3 + time_in_sec4 + time_in_sec5) / 5.0 as avg_duration from data4 
            where  sf = 1
) 
select 'Q' || query_id as query_id, sf, ablation, avg_duration * 1000 as avg_duration 
from data where query_id in (1,4,14,15,17)
""").fetchdf()


#no_indexes = no_indexes[no_indexes.queryId.isin(query_list)]
#no_indexes = no_indexes[['queryId', 'avg_duration']]
#no_indexes['ablation'] = ['No Indexes' for _ in range(len(no_indexes))]
#zone_maps = zone_maps[zone_maps.queryId.isin(query_list)]
#zone_maps = zone_maps[['queryId', 'avg_duration']]
#zone_maps['ablation'] = ['Zone Maps' for _ in range(len(zone_maps))]
#smokedduck = smokedduck[smokedduck.queryId.isin(query_list)]
#smokedduck = smokedduck[['queryId', 'avg_duration']]
#smokedduck['ablation'] = ['Group By Indexes' for _ in range(len(smokedduck))]
#postprocess = postprocess[postprocess['ablation'] == -1]
#postprocess = postprocess[postprocess['sf'] == 1]
#postprocess = postprocess[postprocess.query_id.isin(query_list)]
#postprocess = postprocess.reset_index()
#postprocess['avg_duration'] = [(
#                                   postprocess['time_in_sec1'][i] +
#                                   postprocess['time_in_sec2'][i] +
#                                   postprocess['time_in_sec3'][i] +
#                                   postprocess['time_in_sec4'][i] +
#                                   postprocess['time_in_sec5'][i]
#                               ) / 5 for i in range(len(postprocess))]
#postprocess = postprocess[['query_id', 'avg_duration']]
#postprocess['ablation'] = ['Group By Postprocess' for _ in range(len(postprocess))]
#postprocess = postprocess.rename(columns={'query_id': 'queryId'})
#print(no_indexes)
#print(zone_maps)
#print(smokedduck)
#print(postprocess)
#
#data = no_indexes.append(zone_maps).append(smokedduck).append(postprocess)
#data['queryId'] = [query_list_to_pos_map[q] for q in data['queryId']]
#data['queryId'] = data['queryId'].astype('str')
#data['avg_duration'] = data['avg_duration'].mul(1000)
#data = data.rename(columns={'queryId': 'Query', 'avg_duration': 'Runtime'})
#
print(data)



postfix = """
data$ablation = factor(data$ablation, levels=c('No Indexes', '+Zone Maps', '+Group By Idxs', 'Postprocess'))
data$query_id = factor(data$query_id, levels=c('Q1', 'Q4', 'Q14', 'Q15', 'Q17'))
"""
p = ggplot(data, aes(x='query_id', ymin=0, ymax='avg_duration', y='avg_duration', condition='ablation', color='ablation', fill='ablation', group='ablation', factor='avg_duration')) 
p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) 
p += geom_point(aes(shape='ablation'), position=position_dodge(width=0.6), width=0.5, size=2) 
p += scale_y_log10(name=esc("Runtime (log)"), breaks=[1, 10, 100, 1000, 10000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1s'), esc('10s')]) 
p += scale_x_discrete(name=esc("TPC-H Query"))
p += scale_color_discrete(name=esc("ablation") )
p += scale_fill_discrete(name=esc("ablation") )
p += legend_side
ggsave("ablation.png", p, postfix=postfix, width=4.5, height=1.3)

