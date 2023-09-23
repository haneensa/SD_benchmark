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
            (n1)::int as n1, n2, sel, ncol, skew, groups as g, output,
           index_join,  lineage_type as System, query as op_type,
           greatest(0, {prefix}overhead) as overhead, greatest(0, {prefix}roverhead) as roverhead
    FROM {table}"""

template = f"""
  WITH data as (
    {mktemplate('Total', 'plan_all_', 'micro_sd_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'plan_mat_', 'micro_sd_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'plan_mat_', 'micro_perm_metrics')}
    UNION ALL
    {mktemplate('Total', 'plan_all_', 'micro_perm_metrics')}
    UNION ALL
    {mktemplate('Execute', 'plan_execution_', 'micro_sd_metrics')}
    UNION ALL
    {mktemplate('Execute', 'plan_execution_', 'micro_perm_metrics')}
  ) SELECT * FROM data {"{}"} ORDER BY overheadType desc """

def PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, wrap=None, xkwargs=None, labeller=None):
    print("Plot:")
    print("Plot:")
    print(pdata)
    if linetype:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color, linetype=linetype, shape=linetype))
        p += geom_point()
    else:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color))
    
    p +=  geom_line()
    if xkwargs:
        p += axis_labels(x_label, y_label, x_type, y_type, xkwargs=xkwargs)
    else:
        p += axis_labels(x_label, y_label, x_type, y_type)

    if facet:
        if labeller:
            p += facet_grid(facet, scales=esc("free_y"), labeller=labeller)
        else:
            p += facet_grid(facet, scales=esc("free_y"))

    if wrap:
        p += facet_wrap(wrap)
    p += legend_bottom
    p += legend_side
    ggsave("figures/"+fname, p,  width=w, height=h, scale=0.8)

y_axis_list = ["roverhead", "overhead"]
y_header = ["Relative\nOverhead %", "Overhead (ms)"]
linetype = "overheadtype"
def plot_scans(con):
    where = f"WHERE op_type IN ('SEQ','ORDER_BY')"
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(lambda x: x.capitalize())
    df['ncol'] = df['ncol'].apply(int)

    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "ncol", "# Cols", "system", "op_type~n1"
        #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        x_type, y_type, y_label = "continuous",  "continueous", "{}".format(y_header[idx])
        fname, w, h = "micro_{}_line_scans.png".format(y_axis), 6, 2.5
        #labeller="labeller(n1=function(x)paste(x,'M', sep=''))")
        #ykwargs=dict(breaks=[0,100,200]), 
        xkwargs=dict(breaks=[1,5,10], labels=list(map(esc,['1','5','10'])))
        PlotLines(df, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, xkwargs)

def plot_filters(con):
    where = f"""
    WHERE op_type IN ('FILTER','SEQ_SCAN')
    """
    df = con.execute(template.format(where)).fetchdf()

    # plot all
    ops = ["FILTER", "SEQ_SCAN"]
    for op in ops:
        for idx, y_axis in enumerate(y_axis_list):
            data = df[df["op_type"] == op]
            data['op_type'] = data['op_type'].apply(lambda x: ' '.join([w.capitalize() for w in x.split('_')]))
            data['op_type'] = data['op_type'].apply(lambda x: "Filter Scan" if x=="Seq Scan" else "Filter")

            x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~n1~ncol"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            fname, w, h = "micro_{}_line_{}.png".format(y_axis, op), 8, 4

            PlotLines(data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
            
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_line_{}_log.png".format(y_axis, op), 8, 4

            PlotLines(data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

            x_axis, x_label, color, facet = "ncol", "Projected Columns", "system", "~n1~sel"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_projection_line_{}.png".format(y_axis, op), 8, 4

            PlotLines(data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
    
    # plot Filters figure: 
    where = f"""
    WHERE op_type IN ('FILTER','SEQ_SCAN') AND 
    n1=10000000 and ncol=0
    """
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(lambda x: ' '.join([w.capitalize() for w in x.split('_')]))
    df['op_type'] = df['op_type'].apply(lambda x: "Filter Scan" if x=="Seq Scan" else "Filter")
    for idx, y_axis in enumerate(y_axis_list):
        #p += facet_grid(".~op_type", labeller="labeller(n1=function(x)paste('Card: ',x,'M',sep=''))")
        xkwargs=dict(labels=list(map(esc, ['0', '.25', '.5', '.75', '1'])))
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", ".~op_type"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_10M_filter.png".format(y_axis, op), 6, 2
        PlotLines(df, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, xkwargs)

def plot_cross():
    #### cross
    # vary n1 of n2
    # both systems dominated by materializations overhead
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "n2", "n1", "system", None
        x_type, y_type, y_label = "continuous", "log10",  "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_cross.png".format(y_axis), 5.5, 2.5
        PlotLines("CROSS_PRODUCT", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_ineq_joins(con):
    # joins inequiality
    d = { "BLOCKWISE_NL_JOIN": "BNL", "PIECEWISE_MERGE_JOIN": "Merge", "NESTED_LOOP_JOIN": "NL"}
    where = f""" WHERE
      op_type in ('NESTED_LOOP_JOIN', 'PIECEWISE_MERGE_JOIN', 'BLOCKWISE_NL_JOIN')  
    """
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(d.get)
    ## 1) X-axis: selectivity Y-axis: relative overhead | overhead; group: n1
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~op_type~n2"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_ineq.png".format(y_axis), 8, 4
        PlotLines(df, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

    where = f""" WHERE
      op_type in ('NESTED_LOOP_JOIN', 'PIECEWISE_MERGE_JOIN', 'BLOCKWISE_NL_JOIN')
      and n2=1000000
    """
    df = con.execute(template.format(where)).fetchdf()
    df['op_type'] = df['op_type'].apply(d.get)
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~op_type"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_10M_1k_sel_line_ineq.png".format(y_axis), 6, 2.5
        PlotLines(df, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_index_join_mtm(con):
    where = f""" WHERE op_type = 'INDEX_JOIN_mtm'
    """
    d = { "Q_P": "T2-Only", "Q_P,F": "T1&T2"}
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "g", "Groups", "system", "~skew~index_join~n1"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_indexJoin_mtm.png".format(y_axis), 8, 10
        PlotLines(df,  x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
    
    where = f""" WHERE
      op_type = 'INDEX_JOIN_mtm' and n1=100000 and (skew=1 or skew=0)
    """
    df = con.execute(template.format(where)).fetchdf()
    df['index_join'] = df['index_join'].apply(d.get)
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    #df['n1'] = df['n1'].apply(lambda v: v/10000)
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "g", "Groups", "system", "~index_join~skew"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_100K_indexJoin_mtm.png".format(y_axis), 6, 2.75
        PlotLines(df,  x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_index_join():
    ps = ["Q_P,F", "Q_P"]
    for p in ps:
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "n1", "N1", "system", "~skew~index_join~g"
            #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_line_indexJoin.png".format(y_axis), 8, 10

            PlotLines("INDEX_JOIN", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
            
            lambda_function  = lambda x: x['n2']==1000 and x['index_join']==p

            x_axis, x_label, color, facet = "n1", "N1", "system", "~skew"
            #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_{}_10M_1k_line_indexJoin.png".format(y_axis, p), 6, 3

            PlotLines("INDEX_JOIN", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_hash_join_mtm(con):
    ## hash join plots: 
    ## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: n1
    ###### Hash Join M:N, x-axis: g ordered by output size
    where = f""" WHERE
       op_type = 'HASH_JOIN_mtm'
    """
    df = con.execute(template.format(where)).fetchdf()
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "g", "Groups", "system", "~skew~n1"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_hashJoin_mtm.png".format(y_axis), 8, 8

        PlotLines(df, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
    
    where = f""" WHERE
      op_type = 'HASH_JOIN_mtm' and n1=1000000 and (skew=1)
    """
    df = con.execute(template.format(where)).fetchdf()
    df['skew'] = df['skew'].apply(lambda s: f"Skew: {s}")
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "g", "Groups", "system", "~skew"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        x_type, y_type, y_label = "log10",  "continuous", "{}".format(y_header[idx])
        fname, w, h = "micro_{}_line_1M_hashJoin_mtm.png".format(y_axis), 6, 2.75
        PlotLines(df,  x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
        
def plot_hash_join():
    ## hash join plots: 
    ## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: n1
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "n1", "N1", "system", "~skew~g"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_hashJoin.png".format(y_axis), 8, 8

        PlotLines("HASH_JOIN", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
        
        lambda_function  = lambda x: x['n2']==1000
        x_axis, x_label, color, facet = "n1", "N1", "system", "~skew"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_10M_1k_line_hashJoin.png".format(y_axis), 6, 2.5

        PlotLines("HASH_JOIN", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_aggs(con):
    where = f""" WHERE
      overheadType<>'Execute' and (system='Logical' or system='Logical_window' or system='SD') and
      (op_type = 'HASH_GROUP_BY' or op_type = 'WINDOW') and n1 in (1000000, 10000000)
    """
    df = con.execute(template.format(where)).fetchdf()
    df['n1'] = df['n1'].apply(lambda v: v / 1000000)
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "g", "Groups (g)", "system", ".~n1"
        labeller="labeller(n1=function(x)paste('# Tuples:',x,'M',sep=''))"
        x_type, y_type, y_label = "log10", "log10", "{} [log]".format(y_header[idx])
        xkwargs=None # dict(breaks=[10,100,1000], labels=list(map(esc,['10','100','1000'])))
        fname, w, h = "micro_{}_10M_line_reg_agg.png".format(y_axis), 8, 2.5

        PlotLines(df, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, xkwargs, labeller)
    where = f""" WHERE
      overheadType<>'Materialize' and
      (op_type = 'HASH_GROUP_BY' or op_type = 'WINDOW') and n1 in (1000000, 10000000) and g=10
    """
    df = con.execute(template.format(where)).fetchdf()
    df['n1'] = df['n1'].apply(lambda v: v / 1000000)
    for idx, y_axis in enumerate(y_axis_list):
        facet = ".~n1"
        p = ggplot(df, aes(x="system", y=y_axis, color=color, fill=linetype))
        p += geom_bar(stat=esc('identity'), width=0.8)
        p += axis_labels(x_label, y_label, "discrete", y_type)
        p += facet_grid(facet, scales=esc("free_y"))
        p += legend_bottom
        p += legend_side
        fname, w, h = "micro_{}_10M_bar_reg_agg.png".format(y_axis), 10, 6
        p += coord_flip()
        ggsave("figures/"+fname, p,  width=w, height=h, scale=0.8)


detailed_template = """
            op_execution_roverhead as op_e_ro,
            plan_execution_roverhead as p_e_ro,

            op_mat_roverhead as op_m_ro,
            plan_mat_roverhead as p_m_ro,
            
            op_all_roverhead as op_a_ro,
            plan_all_roverhead as p_a_ro
"""
#avg(op_all_overhead) as o_a_o, avg(op_all_roverhead) as o_a_ro,
#avg(op_execution_overhead) as o_e_o, avg(op_execution_roverhead) as o_e_ro,
#avg(op_mat_overhead) as o_m_o, avg(op_mat_roverhead) as o_m_ro,
summary_template = """
        avg(plan_execution_overhead) as p_e_o, avg(plan_execution_roverhead) as p_e_ro,
        stddev_pop(plan_execution_roverhead) as sp_e_ro,
          
        avg(plan_mat_overhead) as p_m_o, avg(plan_mat_roverhead) as p_m_ro,
        stddev_pop(plan_mat_roverhead) as sp_m_ro,
        
        
        max(plan_execution_roverhead) as maxp_e_ro,
        max(plan_mat_roverhead) as maxp_m_ro,
        
        min(plan_execution_roverhead) as minp_e_ro,
        min(plan_mat_roverhead) as minp_m_ro
"""

header_unique = ["query", "n1", "n2", "skew", "ncol", "sel", "groups",  "index_join"]
g = ','.join(header_unique)

def summary(con, q, select):
    print(f"*** detailed: {q}\n", con.execute(f"""select lineage_type, {select},
        {detailed_template}
        from micro_sd_metrics where query='{q}'
        UNION 
        select lineage_type, {select},
        {detailed_template}
        from micro_perm_metrics where query='{q}' 
        order by lineage_type, {select}
        """).fetchdf().to_string())


    print(f"*** summary {q}\n", con.execute(f"""select index_join,
        {summary_template}
        from micro_sd_metrics where query='{q}'
        GROUP BY index_join
        """).fetchdf())
    print(f"*** summary {q}\n", con.execute(f"""select index_join,
        {summary_template}
        from micro_perm_metrics where query='{q}'
        GROUP BY index_join
        """).fetchdf())
    
    print(f"*** summary {q}\n", con.execute(f"""select index_join,
        avg(sd.plan_all_roverhead) as sd_capture, avg(perm.plan_all_roverhead) as perm_capture,
        avg(perm.plan_all_roverhead) / avg(sd.plan_all_roverhead) as speedup,
        
        avg(sd.plan_all_overhead) as osd_capture, avg(perm.plan_all_overhead) as operm_capture,
        avg(perm.plan_all_overhead) / avg(sd.plan_all_overhead) as ospeedup
        from micro_perm_metrics as perm
        JOIN micro_sd_metrics as sd USING ({g}) 
        where query='{q}'
        GROUP BY index_join
        """).fetchdf())
con = get_db()

#plot_scans(con)
#plot_filters(con)
#plot_index_join()
#plot_index_join_mtm(con)
#plot_cross()
#plot_ineq_joins(con)
#plot_hash_join()
#plot_hash_join_mtm(con)
plot_aggs(con)

######### Summary
#summary(con, "SEQ")
#summary(con, "ORDER_BY")
#summary(con, "FILTER", "n1, sel, ncol")
#summary(con, "SEQ_SCAN")
#summary(con, "INDEX_JOIN_mtm")
#summary(con, "HASH_JOIN_mtm")
#for op_type in ('NESTED_LOOP_JOIN', 'PIECEWISE_MERGE_JOIN', 'BLOCKWISE_NL_JOIN', "CROSS_PRODUCT"):
#    summary(con, op_type)
