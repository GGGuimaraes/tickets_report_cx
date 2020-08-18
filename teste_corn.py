#!/home/ubuntu/anaconda3/bin/python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

from babel.numbers import decimal, format_decimal
import utils
from blupy.subject.basic_client_infos import basic_client_infos as clients
from blupy.connections.db_connections import query_in_db as query_in_db


# In[2]:


def basic_client_infos(store_id = (12,80,115), department_id = (6,7,8,9,45,36)):

    query = """select c1.client_id  as client_id, 

    min(ct.happened_at) first_sell,
    max(ct.happened_at) last_sell,
    sum(gross_value) total_sell

    from client_transactions ct


    inner join clients c1 on c1.id = ct.client_id

    where ct.transaction_category_id in (92,93)
    and status = 'confirmed'
    and c1.distributor = False
    and c1.seller = False
    and c1.store_id in {stores}
    and happened_at > '2020-06-30'
    and c1.department_id in {departaments}

    group by c1.client_id""".format(departaments = department_id,stores = store_id)

    base = query_in_db(query, 'pagnet_read_replica.json')     

    return base

# In[3]:


base = basic_client_infos(store_id = (12,80,115,149), department_id = (9,45,36,25,5,6,7,8,9,46,47,149))


# In[4]:


base.to_csv('cliente_score/client_score_bb/codes/base.csv', index = False)

