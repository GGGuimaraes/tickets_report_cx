#!/home/ubuntu/anaconda3/bin/python
# coding: utf-8
# In[1]:


import boto3
import sys
import pandas as pd
import json
from sqlalchemy import create_engine
import time
S3 = boto3.resource('s3')


def extract_file_as_string(path):
    s3_object = S3.Object('blu-etl', path)
    return s3_object.get()['Body'].read().decode('utf-8')



def connect(credentials_name):
    credentials = json.loads(extract_file_as_string(
        'credentials/' + credentials_name))
    
    return create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'
                        .format(credentials['user'], 
                                credentials['passwd'],
                                credentials['host'],
                                credentials['port'],
                                credentials['dbname']))




# In[2]:


def possible_buys(c):
    
    forn_direcionado=forn_interesse[forn_interesse.client_id == c.client_id]
    return forn_direcionado[forn_direcionado.mean_ticket < c.future_balance].shape[0]
    


# In[3]:


import pandas as pd
import psycopg2
import pandas.io.sql as sqlio
import numpy as np
import os,sys, inspect
import db_connections
import boto3
import utils
from datetime import datetime
from s3 import *

import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

con_destiny = db_connections.connect('blu-rds-datascience.json')
con_pipe = db_connections.connect('blu_datascience.json')
con_pagnet_production = db_connections.connect('pagnet_read_replica.json')
#con_ds_redshift= db_connections.connect('dw_redshift.json')
con_send=connect('blu-rds-datascience.json')


# In[4]:


def client_score_first_bad_impressions():
    
    users = utils.select_users()
    base = utils.basic_client_infos_conclusao_de_venda()
    pos = utils.pos_count_by_client_id()
        
    #base = base[base.churn == 0]
    
    base = base[(base.ativated_at <= pd.Timestamp('2020-07-20')) & (base.total_sell <= 500) & (base.ativated_at >= pd.Timestamp('2020-04-01'))] 
    wallet_defined=pd.read_csv('wallet_defined.csv')    
    base=base[~base.client_id.isin(wallet_defined.client_id)]
    base=base[base.client_id.isin(pos.client_id)]

    base['cs'] = 1
    return base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']]


# In[5]:


x= client_score_first_bad_impressions()


# In[6]:


def past_tickets(data, days = 20):

    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_consegui_contato_, zendeskusers.email, zendesktickets.created_at from zendesktickets
    
    join zendeskusers on zendeskusers.id = zendesktickets.requester_id
    
    where (custom_fields_consegui_contato_ = 'sim__consegui_contato_com_os_números'
    or custom_fields_conseguiu_falar_com_o_cliente_ = 'sim__consegui_contato_com_os_números') """
    ticket_not_talked = sqlio.read_sql_query(query, con_pipe)
    ticket_not_talked['id_zendesk_ticket'] = ticket_not_talked.id_zendesk_ticket.astype('int')


    ticket_not_talked['created_at_past']=(pd.Timestamp.now() - ticket_not_talked.created_at).dt.days.fillna(9999)
    
    ticket_not_talked = ticket_not_talked.sort_values('created_at_past',ascending= True).drop_duplicates('email', keep = 'first')

    ticket_not_talked=ticket_not_talked[ticket_not_talked.created_at_past < days]

    data=data[~data.email.isin(ticket_not_talked.email)]
    return data



# In[7]:


def coleta(data, days = 20):

    query = """select zendeskusers.email  from zendesktickets

            inner join zendeskusers on zendeskusers.id = zendesktickets.requester_id

            where  ticket_form_id = 360000290011 """
    
    coletas = sqlio.read_sql_query(query, con_pipe)
    
    data=data[~data.email.isin(coletas.email)]
    return data


# In[8]:


def big_clients_priority_call(data):
    
    
    query = """ select client_id, count(id) cnt from clients
    group by client_id"""
    
    group_by_clients = sqlio.read_sql_query(query, con_pagnet_production)
    

    data = data.merge(group_by_clients, on = 'client_id')
    data['cs']= data ['cs'] + data['cnt']/10
    
    data.drop('cnt', axis = 1, inplace = True)
               
    
    return data


# In[9]:


def do_not_talk_before_needed(data): #make sure that the next touchpoint will happens just on the set day 
    
    data_to_call='2020-0' + str(pd.Timestamp.now().month) + '-' + str(pd.Timestamp.now().day)

    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_data_para_novo_contato,  date_trunc('days', GETDATE() ) from zendesktickets
    inner join zendeskusers on zendesktickets.assignee_id = zendeskusers.id
    where
    custom_fields_data_para_novo_contato = '{}'
    and ticket_form_id = '360000699092'
    """.format(data_to_call)
    priority_call_today = sqlio.read_sql_query(query, con_pipe)


    priority_call_today['id_zendesk_ticket'] = priority_call_today.id_zendesk_ticket.astype('int')


    query = """select client_id, created_at created_at_past, id_zendesk_ticket from zendesk_integration
    where created_at > '2020/01/09'"""
    opened_tickets = sqlio.read_sql_query(query, con_destiny)


    priority_call_today = priority_call_today.merge(opened_tickets, on = 'id_zendesk_ticket')
    priority_call_today=priority_call_today.sort_values('created_at_past', ascending = False).drop_duplicates('client_id', keep = 'first')


    data['cs']=np.where((data.client_id.isin(priority_call_today.client_id)), data.cs + 9999, data.cs)
    
    return data


# In[10]:


def punish_recall(data):
    #### Revisitar ######
    
    query = """select client_id, created_at created_at_past, id_zendesk_ticket from zendesk_integration

    where created_at > '2020/01/09'
    and formulario_id = '360000699092'"""

    opened_tickets = sqlio.read_sql_query(query, con_destiny)

    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_consegui_contato_ from zendesktickets
            where custom_fields_consegui_contato_ = 'não__não_consegui_contato_com_esses_números'
            and ticket_form_id = '360000699092' """

    ticket_not_talked = sqlio.read_sql_query(query, con_pipe) 
    ticket_not_talked['id_zendesk_ticket'] = ticket_not_talked.id_zendesk_ticket.astype('int')

    lost_hope=opened_tickets.merge(ticket_not_talked, on= 'id_zendesk_ticket')

    losing_hope = lost_hope.groupby('client_id', as_index = False).count()[['client_id','created_at_past']]

    data = data.merge(losing_hope, on = 'client_id', how = 'left' )
    data.created_at_past.fillna(0,inplace = True)
    data['cs'] = data['cs'] - data['created_at_past'] * 0.25
    data.drop('created_at_past', axis = 1, inplace = True)

    return data


# In[11]:


def came_from_rep100_campain_priority_call(data):
    
    
    query = """select client.client_cpf_cnpj as cpf_cnpj from Campanha_rep1000_sellers_recommendations as rep_client

                inner join Campanha_rep1000_sellers as rep on rep_client.seller_cpf_cnpj = rep.cpf_cnpj 
                inner join Campanha_rep1000_recommendations client on  rep_client.client_cpf_cnpj = client.client_cpf_cnpj"""

    came_from_femur = sqlio.read_sql_query(query, con_ds_redshift)

    

    query = """select client.client_cpf_cnpj as cpf_cnpj, client_id from clients
    
    where distributor = True 
    and seller = False"""

    cpf_cnpj_client_id = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    came_from_femur=came_from_femur.merge(cpf_cnpj_client_id, on = 'client_id')
    
    
    
    

    data = data.merge(group_by_clients, on = 'client_id')
    data['cs']= data ['cs'] + data['cnt']/10
    
    data.drop('cnt', axis = 1, inplace = True)
               
    
    return data


# In[12]:


def today_tickets(data):
    
    query = """select client_id, created_at created_at_past, id_zendesk_ticket from zendesk_integration
    where created_at > date_trunc('days',now())"""

    opened_ticket_today = sqlio.read_sql_query(query, con_destiny)
    
    data=data[~data.client_id.isin(opened_ticket_today.client_id)]

    return data

    


# In[13]:


def insert_data(data, n = 0):
    
    data['created_at'] =  pd.Timestamp.now()
    data['atribuído'] =  'Conclusão de Venda'
    data['ticket_name'] = 'Comece a economizar com a Blu!'
    data['atribuido_id'] = 360008079872
    data['texto_html'] =  data['texto']
    data['formulario'] =  'Conclusão de Venda'
    data['formulario_id'] =  360000699092
    data['status'] = 'to_send'
    data['id_zendesk_ticket'] =  np.nan
    data['zendesk_ticket_created_at'] = np.nan
    data['zendesk_user_id'] = np.nan
    data['can_be_send'] = True
    data['subject_kind_id'] = 360030912911
    
    data = data.sort_values('cs', ascending = False) 
    data.tail(n).to_sql('zendesk_integration', con_send, index=False, if_exists='append')


# In[14]:


first_bad_clients=client_score_first_bad_impressions()
print(len(first_bad_clients))

text_as_html=pd.read_csv('text_as_html.csv')
text_as_html = text_as_html[['client_id','text']]

first_bad_clients = first_bad_clients.merge(text_as_html , on = 'client_id')
first_bad_clients.rename({'text':'texto'},axis = 1, inplace = True)

print(len(first_bad_clients))

first_bad_clients = big_clients_priority_call(first_bad_clients)
print(len(first_bad_clients))

first_bad_clients=coleta(first_bad_clients)
print(len(first_bad_clients))


first_bad_clients = past_tickets(first_bad_clients)
print(len(first_bad_clients))

first_bad_clients = do_not_talk_before_needed(first_bad_clients)
print(len(first_bad_clients))

first_bad_clients = today_tickets(first_bad_clients)
print(len(first_bad_clients))

first_bad_clients = punish_recall(first_bad_clients)
print(len(first_bad_clients))


# In[15]:


query = """select email email, user_fields_gestor_rentabilizacao from zendeskusers

where 1=1
and phone is not null
"""

x = sqlio.read_sql_query(query, con_pipe)


# In[16]:


query = """select client_id, created_at created_at_past, id_zendesk_ticket from zendesk_integration
where created_at > '2020-07-15'
"""

opened_ticket = sqlio.read_sql_query(query, con_destiny)


# In[17]:


first_bad_clients_x = first_bad_clients[~first_bad_clients.client_id.isin(opened_ticket.client_id)]


# In[18]:


first_bad_clients_x = first_bad_clients[first_bad_clients.email.isin(x.email)]


# In[19]:


first_bad_clients_x = first_bad_clients_x.rename({'text':'texto'})


# In[20]:


first_bad_clients_x = first_bad_clients_x.rename(columns={'text':'texto'})


# In[21]:


first_bad_clients_x['cx_owner'] = 'thais_carvalho'


# In[22]:


len(first_bad_clients_x)


# In[23]:


insert_data(first_bad_clients_x, n =1)


# In[24]:


#################################################################################
#############################################################################


# In[ ]:




