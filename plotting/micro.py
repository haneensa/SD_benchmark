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
})

def PlotLines(op, lfunction, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, wrap=None):
    if op is not None:
        pdata =  [d for d in data if d['op_type']==op and lfunction(d)]
    else:
        pdata =  [d for d in data if lfunction(d)]
    print(pdata)
    if linetype:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color, linetype=linetype))
        p += geom_point()
    else:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color))
    
    p +=  geom_line(stat=esc('identity'), alpha=0.8, width=0.5)
    p += axis_labels(x_label, y_label, x_type, y_type)
    if facet:
        p += facet_grid(facet, scales=esc("free_y"))
    if wrap:
        p += facet_wrap(wrap)
    p += legend_bottom
    p += legend_side
    ggsave(fname, p,  width=w, height=h, scale=0.8)

y_axis_list = ["roverhead", "overhead"]
y_header = ["Relative\nOverhead %", "Overhead (ms)"]
linetype = "overheadType"
true_function  = lambda x: True
def plot_scans():
    ops = ["scan", "orderby"]
    for op in ops:
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "p", "# cols", "system", "~n1"
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            x_type, y_type, y_label = "continuous",  "continueous", "{}".format(y_header[idx])
            fname, w, h = "micro_{}_line_{}.png".format(y_axis, op), 8, 3
            lambda_function  = lambda x: True
            PlotLines(op, true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_filters():
    ops = ["filter", "filter_scan"]
    for op in ops:
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~n1~p"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_line_{}.png".format(y_axis, op), 8, 4

            PlotLines(op, true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

            x_axis, x_label, color, facet = "p", "Projected Columns", "system", "~n1~sel"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_projection_line_{}.png".format(y_axis, op), 8, 4

            PlotLines(op, true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

            lambda_function  = lambda x: x['n1'] == 10000000 and x['p']==3
            x_axis, x_label, color, facet= "sel", "Selectivity", "system", None#"~p"
            x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_10M_p3_line_{}.png".format(y_axis, op), 5, 2.5

            PlotLines(op, lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_cross():
    #### cross
    # vary n1 of n2
    # both systems dominated by materializations overhead
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "n2", "n1", "system", None
        x_type, y_type, y_label = "continuous", "log10",  "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_cross.png".format(y_axis), 5.5, 2.5
        PlotLines("cross", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_ineq_joins():
    # joins inequiality
    ## 1) X-axis: selectivity Y-axis: relative overhead | overhead; group: n1
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~op_type~n2"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_ineq.png".format(y_axis), 8, 4
        lambda_function  = lambda x: x['op_type'] in ["nl", "merge", "bnl"]
        PlotLines(None, lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

    for idx, y_axis in enumerate(y_axis_list):
        lambda_function  = lambda x: x['op_type'] in ["nl", "merge", "bnl"] and x['n2']==1000000
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~op_type"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_10M_1k_line_ineq.png".format(y_axis), 6, 2.5
        PlotLines(None, lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_index_join():
    ps = ["Q_P,F", "Q_P"]
    for p in ps:
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew~index_join~g~n1"
            #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_line_indexJoin.png".format(y_axis), 8, 10

            PlotLines("index_join_pkfk", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
            
            lambda_function  = lambda x: x['n1'] == 10000000 and x['n2']==1000 and x['index_join']==p

            x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew~g"
            #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = "micro_{}_{}_10M_1k_line_indexJoin.png".format(y_axis, p), 6, 3

            PlotLines("index_join_pkfk", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_hash_join():
    ## hash join plots: 
    ## 1) X-axis: selectivity, Y-axis: relative overhead | overhead; group: n1
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew~n1~n2"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        #x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_hashJoin.png".format(y_axis), 8, 8

        PlotLines("hash_join_pkfk", true_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
        
        lambda_function  = lambda x: x['n1'] == 10000000 and x['n2']==1000
        x_axis, x_label, color, facet = "sel", "Selectivity", "system", "~skew"
        #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_10M_1k_line_hashJoin.png".format(y_axis), 6, 2.5

        PlotLines("hash_join_pkfk", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

def plot_aggs():
    for idx, y_axis in enumerate(y_axis_list):
        lambda_function  = lambda x: x['n1'] == 10000000
        x_axis, x_label, color, facet = "g", "Groups (g)", "system", None
        x_type, y_type, y_label = "continuous", "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_10M_line_reg_agg.png".format(y_axis), 4, 2.5

        PlotLines("reg_agg", lambda_function, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)

con = get_db()

df = con.execute("""select * from micro_sd_metrics union all select * from micro_perm_metrics""").fetchdf()
data = []
for index, row in df.iterrows():
    data.append(dict(overheadType="mat", n1=row['n1'], n2=row['n2'], sel=row['sel'], p=row['p'], skew=row['skew'], g=row['groups'],
                     index_join=row['index_join'], system=row['lineage_type'], op_type=row['query'],
                     overhead=max(row['mat_overhead'], 0), roverhead=max(row['mat_rel_overhead'], 0)))
    data.append(dict(overheadType="exec", n1=row['n1'], n2=row['n2'], sel=row['sel'], p=row['p'], skew=row['skew'], g=row['groups'],
                     index_join=row['index_join'], system=row['lineage_type'], op_type=row['query'],
                     overhead=max(row['exec_overhead'], 0), roverhead=max(row['exec_rel_overhead'], 0)))
    #data.append(dict(overheadType="all", n1=row['n1'], n2=row['n2'], sel=row['sel'], p=row['p'], skew=row['skew'], g=row['groups'],
    #                 index_join=row['index_join'], system=row['lineage_type'], op_type=row['query'],
    #                 overhead=max(row['overhead'], 0), roverhead=max(row['rel_overhead'], 0)))


        
plot_scans()
plot_filters()
plot_cross()
plot_ineq_joins()
plot_index_join()
plot_hash_join()
plot_aggs()
