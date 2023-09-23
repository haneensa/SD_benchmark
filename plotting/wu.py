import numpy as np
import pandas as pd
from pygg import *
import json
from process_data import get_db

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
  "legend.margin":"margin(t = 0, unit='cm')"
})


def mktemplate(overheadType, prefix, table):
    return f"""
    SELECT '{overheadType}' as overheadType, 
            (n1)::int as n1, n2, sel, ncol, skew, groups as g, output,
           index_join,  lineage_type as System, query as op_type,
           greatest(0, {prefix}overhead) as overhead, greatest(0, {prefix}roverhead) as roverhead
    FROM {table}"""
 
#    {mktemplate('Materialize', 'mat_', 'micro_sd_metrics')}
#    UNION ALL
#    {mktemplate('Execute', 'exec_', 'micro_sd_metrics')}
#    UNION ALL

con = get_db()
template = f"""
  WITH data as (
    {mktemplate('Total', 'plan_all_', 'micro_sd_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'plan_mat_', 'micro_sd_metrics')}
    UNION ALL
    {mktemplate('Execute', 'plan_execution_', 'micro_sd_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'plan_mat_', 'micro_perm_metrics')}
    UNION ALL
    {mktemplate('Execute', 'plan_execution_', 'micro_perm_metrics')}
    UNION ALL
    {mktemplate('Total', 'plan_all_', 'micro_perm_metrics')}
  ) SELECT * FROM data {"{}"} ORDER BY overheadType desc """


execute = "op_exec"
if 1:
    where = f"WHERE overheadType <> '{execute}' AND op_type IN ('SEQ','ORDER_BY')"
    df = con.execute(template.format(where)).fetchdf()
    print(df)
    df['op_type'] = df['op_type'].apply(lambda x: x.capitalize())
    df['ncol'] = df['ncol'].apply(int)
    p = ggplot(df, aes(x='ncol', color='system', y='roverhead', linetype='overheadtype', shape='system')) 
    p += facet_grid("op_type~n1", labeller="labeller(n1=function(x)paste(x,'M', sep=''))")
    p += axis_labels("# Cols", "Relative\nOverhead %", ykwargs=dict(breaks=[0,100,200]), xkwargs=dict(breaks=[1,5,10], labels=list(map(esc,['1','5','10']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/micro_overhead_line_scanorderby.png", p, width=6, height=2.5, scale=0.8)

    where = f"""
    WHERE overheadType <> '{execute}' AND op_type IN ('FILTER','SEQ_SCAN') AND 
    n1=10000000 and ncol=8
    """
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(lambda x: ' '.join([w.capitalize() for w in x.split('_')]))
    df['op_type'] = df['op_type'].apply(lambda x: "Filter Scan" if x=="Seq Scan" else "Filter")
    p = ggplot(df, aes(x='sel', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid(".~op_type", labeller="labeller(n1=function(x)paste('Card: ',x,'M',sep=''))")
    p += axis_labels("Selectivity", "Relative\nOverhead %", xkwargs=dict(labels=list(map(esc, ['0', '.25', '.5', '.75', '1']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_line_filter.png", p, width=6, height=2, scale=0.8)



    where = f""" WHERE
      overheadType <> '{execute}' AND op_type = 'HASH_GROUP_BY' and n1 in (1000000, 10000000)
    """
    d = { "Q_P": "PK-Only", "Q_P,F": "PKFK"}
    df = con.execute(template.format(where)).fetchdf()
    p = ggplot(df, aes(x='g', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid(".~n1", scales=esc('free'),
                    labeller="labeller(n1=function(x)paste('# Tuples:',x,'M',sep=''))")
    p += axis_labels("# Groups (g)", "Relative\nOverhead (log)", "log10", "log10")
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_10M_line_reg_agg.png", p, width=6, height=2, scale=0.8)

    where = f""" WHERE
      overheadType <> '{execute}' AND op_type in ('NESTED_LOOP_JOIN', 'PIECEWISE_MERGE_JOIN', 'BLOCKWISE_NL_JOIN')  
    """
    d = { "BLOCKWISE_NL_JOIN": "BNL", "PIECEWISE_MERGE_JOIN": "Merge", "NESTED_LOOP_JOIN": "NL"}
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(d.get)
    df['n2'] = df['n2'].apply(lambda v: v / 1000)
    p = ggplot(df, aes(x='n2', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("sel~op_type", scales=esc('free'))
    p += axis_labels("|T2| (log)", "Relative\nOverhead (log)", "log10", "log10", xkwargs=dict(breaks=[1, 10, 100, 1000],labels=list(map(esc, ['1K', '10K', '100K', '1M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_all_line_ineqjoin.png", p, width=7, height=4, scale=0.8)
    
    where = f""" WHERE
      overheadType <> '{execute}' AND op_type in ('NESTED_LOOP_JOIN', 'PIECEWISE_MERGE_JOIN', 'BLOCKWISE_NL_JOIN')  
      and n2=1000000
    """
    d = { "BLOCKWISE_NL_JOIN": "BNL", "PIECEWISE_MERGE_JOIN": "Merge", "NESTED_LOOP_JOIN": "NL"}
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(d.get)
    #df['n2'] = df['n2'].apply(lambda v: v / 1000)
    p = ggplot(df, aes(x='sel', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("~op_type", scales=esc('free'))
    p += axis_labels("Selectivity", "Relative\nOverhead (log)", "log10", "log10", xkwargs=dict(breaks=[1, 10, 100, 1000],labels=list(map(esc, ['1K', '10K', '100K', '1M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_10M_line_ineqjoin.png", p, width=7, height=4, scale=0.8)
    
    where = f""" WHERE
      overheadType <> '{execute}' AND op_type in ('NESTED_LOOP_JOIN', 'PIECEWISE_MERGE_JOIN', 'BLOCKWISE_NL_JOIN')  
      and sel=0.5
    """
    d = { "BLOCKWISE_NL_JOIN": "BNL", "PIECEWISE_MERGE_JOIN": "Merge", "NESTED_LOOP_JOIN": "NL"}
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(d.get)
    df['n2'] = df['n2'].apply(lambda v: v / 1000)
    p = ggplot(df, aes(x='n2', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("~op_type", scales=esc('free'))
    p += axis_labels("|T2| (log)", "Relative\nOverhead (log)", "log10", "log10", xkwargs=dict(breaks=[1, 10, 100, 1000],labels=list(map(esc, ['1K', '10K', '100K', '1M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_sel0.5_line_ineqjoin.png", p, width=7, height=4, scale=0.8)



    where = f""" WHERE
      overheadType <> '{execute}' AND op_type = 'INDEX_JOIN' ANd
      n2=10000
    """
    d = { "Q_P": "P-Only", "Q_P,F": "P&F"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    df['n1'] = df['n1'].apply(lambda v: v / 1000000)
    p = ggplot(df, aes(x='n1', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("index_join~skew", scales=esc('free'))
    p += axis_labels("|N1|", "Relative\nOverhead %", xkwargs=dict(breaks=[1, 5, 10],labels=list(map(esc, ['1M', '5M', '10M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_g10k_line_indexJoin.png", p, width=6, height=2.75, scale=0.8)



    where = f""" WHERE
      overheadType <> '{execute}' AND op_type = 'HASH_JOIN' ANd
      n2=10000
    """
    d = { "Q_P": "PK-Only", "Q_P,F": "PKFK"}
    df = con.execute(template.format(where)).fetchdf()
    print(df)
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    df['n1'] = df['n1'].apply(lambda v: v / 1000000)
    p = ggplot(df, aes(x='n1', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid(".~skew", scales=esc('free'))
    p += axis_labels("|N1|", "Relative\nOverhead %", xkwargs=dict(breaks=[1, 5, 10],labels=list(map(esc, ['1M', '5M', '10M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_g10k_line_hashJoin.png", p, width=6, height=2, scale=0.8)


    where = f""" WHERE
      overheadType <> '{execute}' AND op_type = 'INDEX_JOIN_mtm'
    """
    d = { "Q_P": "P-Only", "Q_P,F": "P&F"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    df['n1'] = df['n1'].apply(lambda v: v/10000)
    df['sel2'] = df["output"]
    p = ggplot(df, aes(x='sel2', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("index_join~skew~n1", scales=esc('free'))
    p += axis_labels("|output|", "Relative\nOverhead %", "log10", "log10", xkwargs=dict(breaks=[1, 10, 100],labels=list(map(esc, ['10K', '100K', '1M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_all_line_indexJoin_mtm.png", p, width=6, height=10, scale=0.8)
    
    where = f""" WHERE
      overheadType <> '{execute}' AND op_type = 'INDEX_JOIN_mtm' and n1=100000 and skew=1
    """
    d = { "Q_P": "P-Only", "Q_P,F": "P&F"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    df['n1'] = df['n1'].apply(lambda v: v/10000)
    df['sel2'] = df["output"]
    df['sel2'] = df['sel2'].apply(lambda v: v/10000)
    print(df['sel2'])
    p = ggplot(df, aes(x='sel2', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("~index_join", scales=esc('free'))
    p += axis_labels("|output|", "Relative\nOverhead %", "continuous", "log10", xkwargs=dict(breaks=[10, 20, 30, 40],labels=list(map(esc, ['100M', '200M', '300M', '400M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_sel_line_indexJoin_mtm.png", p, width=6, height=2.75, scale=0.8)
    
    ###### Hash Join M:N, x-axis: g ordered by output size
    where = f""" WHERE
      overheadType <> '{execute}' AND op_type = 'HASH_JOIN_mtm'
    """
    d = { "Q_P": "P-Only", "Q_P,F": "P&F"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['n1'] = df['n1'].apply(lambda v: v/10000)
    print(df["skew"])
    df['sel2'] = df["output"]
    p = ggplot(df, aes(x='sel2', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("n1~skew", scales=esc('free'))
    p += axis_labels("|N1|", "Relative\nOverhead %")
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_all_line_hash_joinJoin_mtm.png", p, width=6, height=2.75, scale=0.8)
    
    ###### Hash Join M:N, N1=1M and skew=1. x-axis: output size
    where = f""" WHERE
      overheadType <> '{execute}' AND op_type = 'HASH_JOIN_mtm' and n1=1000000 and skew=1
    """
    d = { "Q_P": "P-Only", "Q_P,F": "P&F"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['sel2'] = df["output"]
    df['n1'] = df['n1'].apply(lambda v: v/10000)
    df['sel2'] = df['sel2'].apply(lambda v: v/10000000)
    print(df["sel2"])
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    print(df["skew"])
    p = ggplot(df, aes(x='sel2', color='system', y='roverhead', linetype='overheadtype', shape='system'))
    p += facet_grid("~skew")
    p += axis_labels("|output|", "Relative\nOverhead %", xkwargs=dict(breaks=[10, 20, 30, 40],labels=list(map(esc, ['100M', '200M', '300M', '400M']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("figures/wu_micro_overhead_sel_line_hash_joinJoin_mtm.png", p, width=6, height=2.75, scale=0.8)



# for each query,
data = pd.read_csv('eval_results/lineage_ops_4_9_2023_with_rand_and_skew.csv')
ops = {
    'groupby',
    'filter',
    'perfgroupby',
    'hashjoin',
    'mergejoin',
    'nljoin',
    'simpleagg',
    'orderby',
}
order_map = {
    'simpleagg': 'Simple Agg',
    'orderby': 'Order By',
    'filter': 'Filter',
    'groupby': 'Group By',
    'perfgroupby': 'Perfect GrpBy',
    'nljoin': 'NL Join',
    'mergejoin': 'Merge Join',
    'hashjoin': 'Hash Join',
}
category = {
    'simpleagg': 'agg',
    'orderby': 'misc',
    'filter': 'misc',
    'groupby': 'agg',
    'perfgroupby': 'agg',
    'nljoin': 'Join',
    'mergejoin': 'Join',
    'hashjoin': 'Join',
}




data = data[data['avg_parse_time'] != 0]
data = data[data['oids'] == 1000]
data['avg_duration'] = data['avg_duration'] - data['avg_parse_time'] # Subtract out parse time
data = data[['oids', 'avg_duration', 'op']]
data = data[data['op'].isin(ops)]
data['category'] = data['op'].apply(category.get)
data['op'] = data['op'].apply(order_map.get)
data['avg_duration'] = data['avg_duration'].mul(1000)
data = data.rename(columns={'oids': 'Queried_ID_Count', 'avg_duration': 'Runtime'})
postfix = """   
data$op = factor(data$op, levels=c("Filter", "Order By", "Simple Agg", "Group By", "Perfect GrpBy", "NL Join", "Merge Join", "Hash Join"))
"""
p = ggplot(data, aes(x='op', y='Runtime', color='category', fill='category'))
p += geom_bar(stat=esc('identity'), width=0.8)
p += axis_labels("", "Runtime/oid (ms)", "discrete")
#p += facet_wrap(".~category", scales=esc("free"))
p += legend_none
p += coord_flip()
ggsave("figures/lq_microbench.png", p, postfix=postfix, width=6, height=2, scale=0.8)


