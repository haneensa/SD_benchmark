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

legend_side = legend + theme(**{
  "legend.position":esc("right"),
  "legend.margin":"margin(t = 0, unit='cm')"
})


def mktemplate(overheadType, prefix, table):
    return f"""
    SELECT '{overheadType}' as overheadType, 
            (n1/1000000)::int as n1, n2, sel, p, skew, groups as g,
           index_join, lineage_type as System, query as op_type,
           greatest(0, {prefix}overhead) as overhead, greatest(0, {prefix}rel_overhead) as roverhead
    FROM {table}"""
 
#    {mktemplate('Materialize', 'mat_', 'micro_sd_metrics')}
#    UNION ALL
#    {mktemplate('Execute', 'exec_', 'micro_sd_metrics')}
#    UNION ALL

con = get_db()
template = f"""
  WITH data as (
    {mktemplate('Total', '', 'micro_sd_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'mat_', 'micro_perm_metrics')}
    UNION ALL
    {mktemplate('Execute', 'exec_', 'micro_perm_metrics')}
    UNION ALL
    {mktemplate('Total', '', 'micro_perm_metrics')}
  ) SELECT * FROM data {"{}"} ORDER BY overheadType desc """



if 0:
    where = "WHERE overheadType <> 'Execute' AND op_type IN ('scan','orderby')"
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(lambda x: x.capitalize())
    df['p'] = df['p'].apply(int)
    p = ggplot(df, aes(x='p', color='System', y='roverhead', linetype='overheadType', shape='System')) 
    p += facet_grid("op_type~n1", labeller="labeller(n1=function(x)paste(x,'M', sep=''))")
    p += axis_labels("# Cols", "Relative\nOverhead %", ykwargs=dict(breaks=[0,100,200]), xkwargs=dict(breaks=[1,5,10], labels=list(map(esc,['1','5','10']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("micro_overhead_line_scanorderby.png", p, width=6, height=2.5, scale=0.8)


    where = """
    WHERE overheadType <> 'Execute' AND op_type IN ('filter','filter_scan') AND 
    n1=10 and p=11
    """
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(lambda x: ' '.join([w.capitalize() for w in x.split('_')]))
    p = ggplot(df, aes(x='sel', color='System', y='roverhead', linetype='overheadType', shape='System'))
    p += facet_grid(".~op_type", labeller="labeller(n1=function(x)paste('Card: ',x,'M',sep=''))")
    p += axis_labels("Selectivity", "Relative\nOverhead %", xkwargs=dict(labels=list(map(esc, ['0', '.25', '.5', '.75', '1']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("micro_overhead_line_filter.png", p, width=6, height=2, scale=0.8)


    where = """ WHERE
      overheadType <> 'Execute' AND op_type = 'index_join_pkfk' ANd
      n1 = 10 AND n2=1000
    """
    d = { "Q_P": "P-Only", "Q_P,F": "P&F"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    p = ggplot(df, aes(x='sel', color='System', y='roverhead', linetype='overheadType', shape='System'))
    p += facet_grid("index_join~skew", scales=esc('free'))
    p += axis_labels("Selectivity", "Relative\nOverhead %", xkwargs=dict(labels=list(map(esc, ['0', '.25', '.5', '.75', '1']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("micro_overhead_10M_1k_line_indexJoin.png", p, width=6, height=2.75, scale=0.8)



    where = """ WHERE
      overheadType <> 'Execute' AND op_type = 'hash_join_pkfk' ANd
      n1 = 10 AND n2=1000
    """
    d = { "Q_P": "PK-Only", "Q_P,F": "PKFK"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    p = ggplot(df, aes(x='sel', color='System', y='roverhead', linetype='overheadType', shape='System'))
    p += facet_grid(".~skew", scales=esc('free'))
    p += axis_labels("Selectivity", "Relative\nOverhead %", xkwargs=dict(labels=list(map(esc, ['.2', '.4', '.6', '.8', '1']))))
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("micro_overhead_10M_1k_line_hashJoin.png", p, width=6, height=2, scale=0.8)


    where = """ WHERE
      overheadType <> 'Execute' AND op_type = 'reg_agg' and n1 in (1, 10)
    """
    d = { "Q_P": "PK-Only", "Q_P,F": "PKFK"}
    df = con.execute(template.format(where)).fetchdf()
    p = ggplot(df, aes(x='g', color='System', y='roverhead', linetype='overheadType', shape='System'))
    p += facet_grid(".~n1", scales=esc('free'),
                    labeller="labeller(n1=function(x)paste('# Tuples:',x,'M',sep=''))")
    p += axis_labels("# Groups (g)", "Relative\nOverhead (log)", "log10", "log10")
    p += geom_line() + geom_point()
    p += legend_side
    ggsave("micro_overhead_10M_line_reg_agg.png", p, width=6, height=2, scale=0.8)

where = """ WHERE
  overheadType <> 'Execute' AND op_type in ('nl', 'merge', 'bnl')  
"""
d = { "bnl": "BNL", "merge": "Merge", "nl": "NL"}
df = con.execute(template.format(where)).fetchdf()
df['op_type'] = df['op_type'].apply(d.get)
df['n2'] = df['n2'].apply(lambda v: v / 1000)
print(df)
p = ggplot(df, aes(x='n2', color='System', y='roverhead', linetype='overheadType', shape='System'))
p += facet_grid("sel~op_type", scales=esc('free'))
p += axis_labels("|T2| (log)", "Relative\nOverhead (log)", "log10", "log10", xkwargs=dict(breaks=[1, 10, 100, 1000],labels=list(map(esc, ['1K', '10K', '100K', '1M']))))
p += geom_line() + geom_point()
p += legend_side
ggsave("micro_overhead_10M_line_ineqjoin.png", p, width=7, height=4, scale=0.8)


