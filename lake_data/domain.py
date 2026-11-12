"""清点事件修订和逻辑watermark操作。"""
from collections import Counter
from .io import integer

def summarize(v,root):
    integer(v['gap'],1);integer(v['allowed_lateness'])
    for x in v['operations']:
        if x['kind'] not in ('upsert','delete','watermark'):raise ValueError('源操作未知')
    return {'operations':len(v['operations']),'kinds':dict(sorted(Counter(x['kind'] for x in v['operations']).items())),'gap':v['gap']}
