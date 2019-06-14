import os
from we_module.we import We
import pandas as pd
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth',1000)

rs_conn = 'postgresql://lu.ye@wework.com:Luye@1234@redshift-production.weworkers.io:5439/analyticdb'
os.environ['REDSHIFT_CONN'] = rs_conn

we = We()

"""
segment_sql = '''
select * from playground.page_segment t
where t.timestamp>=date'2019-06-08'
and t.timestamp<date'2019-06-09'
order by t.timestamp
'''
heap_sql = '''
select * from playground.page_heap t
where t.time>=date'2019-06-08'
and t.time<date'2019-06-09'
order by t.time
'''
df_seg = we.get_tbl_query(segment_sql)

df_seg.index = df_seg['id']
df_seg = df_seg.drop(columns=['id'])

df_heap = we.get_tbl_query(heap_sql)
df_heap.index = df_heap['event_id']
df_heap = df_heap.drop(columns=['event_id'])

df_seg.to_csv('seg.csv')
df_heap.to_csv('heap.csv')
"""

# prepare segment data
df_seg = pd.read_csv('seg.csv', index_col='id')
df_seg = df_seg[['timestamp','context_ip','context_user_agent']]
df_seg = df_seg[~df_seg['context_user_agent'].str.contains('Catch')]  # remove catchpoint data
df_seg = df_seg[['timestamp','context_ip']].head(100)

# prepare heap data
df_heap = pd.read_csv('heap.csv', index_col='event_id' )
df_heap = df_heap[['time','ip']]
df_heap = df_heap.head(100)

# for index, row in df_seg_test.iterrows():
#     print(index, row['timestamp'])


