import time
import numpy as np
import datetime
import pandas as pd
import sqlalchemy
# Connector function
def postgres_connector(host, port, database, user, password=None):
    user_info = user if password is None else user + ':' + password
    # example: postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgres://%s@%s:%d/%s' % (user_info, host, port, database)
    return sqlalchemy.create_engine(url, client_encoding='utf-8')
# Get connect engine   
engine = postgres_connector(
   "35.187.144.113",
   5432,
   "intern_task",
   "candidate",
   "dcard-data-intern-2020"
)

query = """
SELECT *
FROM posts_train
"""
hot_train = pd.read_sql(query, engine)

query = """
SELECT *
FROM post_shared_train
"""
shared_train = pd.read_sql(query, engine)

def add_other_c(hot,oth):
    key = hot['post_key']
    hour = hot['created_at_hour']
    
    conform = oth[oth['post_key'] == key]
    
    ac_count = 0
    
    
    if len(conform) >= 1:
        ac_count = conform[conform['created_at_hour']< (hour + datetime.timedelta(hours=36))]['count'].sum()
    return ac_count

# hot_train['share'] = hot_train.apply(lambda x : add_other_c(x, shared_train),axis=1)

s = time.time()
hot_train['share'] = hot_train.apply(lambda x : add_other_c(x,shared_train),axis=1)
e = time.time()
print(e-s)