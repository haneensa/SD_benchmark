import pandas as pd
from pygg import *

# for each query,
filter_micro = pd.read_csv('filter_explor_3_26_2023.csv')
data = filter_micro

print(data)

data['avg_duration'] = data['avg_duration'].mul(1000)
data['num_chunks'] = data['num_records'].div(1024)
data = data.rename(columns={'num_chunks': 'Base_Query_Chunk_Count', 'avg_duration': 'Runtime'})

legend = theme_bw() + theme(**{
    # "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
    # "legend.justification":"c(1,0)",
    # "legend.position":"c(1,0)",
    # "legend.key" : element_blank(),
    # "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=8, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=8),
    # "plot.background": element_blank(),
    # "panel.border": element_rect(color=esc("#e0e0e0")),
    # "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333")),
    "legend.position": esc('none'),
    # "legend.margin": margin(t = 0, r = 0, b = 0, l = 0, unit = esc("pt")),
    # "legend.text": element_text(colour = "'#333333'", size=9, family = "'Arial'"),
    # "legend.key.size": unit(8, esc('pt')),
})

p = ggplot(data, aes(x='Base_Query_Chunk_Count', y='Runtime', condition='op', color='op', fill='op', group='op')) \
    + scale_y_continuous(breaks=[0.8, 1, 1.2, 1.4], labels=[esc('0.8ms'), esc('1ms'), esc('1.2ms'), esc('1.4ms')]) \
    + scale_x_log10(
        name=esc('Base Query Chunk Count (log)'),
        breaks=[1, 100, 10000, 1000000],
        labels=[esc('1'), esc('100'), esc('10000'), esc('1000000')]
    ) \
    + geom_line() \
    + legend
ggsave("big_base_query.png", p, width=2.3, height=1)
#stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) \
# + scale_y_log10() + scale_x_log10() \
