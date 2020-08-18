#!/home/ubuntu/anaconda3/bin/python
# coding: utf-8


# In[1]:


import boto3
import sys
import pandas as pd
import json
from sqlalchemy import create_engine
import time
from blupy.connections.db_connections import query_in_db as query_in_db 
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


import pandas as pd
import psycopg2
import pandas.io.sql as sqlio
import numpy as np
import os,sys, inspect
from sklearn.preprocessing import LabelEncoder
import db_connections
import boto3
from datetime import datetime
from s3 import *
import utils
import client_score_basic_functions as cs

import matplotlib.pyplot as plt

from os import walk

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


def segments_otica(base):

    query = "select client_id from clients where department_id in (36)"
    otica = sqlio.read_sql_query(query, con_pagnet_production)
    base=base[base.client_id.isin(otica.client_id)]
    
    return base


def marchon_historic_base_payload():

    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)
    l=utils.pos_count_by_client_id()
    
    base = base[(base.total_sell > 500)]
    base = base[base.client_id.isin(l.client_id)]
    base=base[base.churn == 0]
    base['cs'] = 1
    base=segments_otica(base)
    
    return base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 

def marchon_payload():

    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)
    l=utils.pos_count_by_client_id()
    
    base = base[(base.total_sell > 500)]
    base = base[base.client_id.isin(l.client_id)]
    base['cs'] = 1
    marchon = pd.read_csv('marchon_clients.csv')
    base = base[base.client_id.isin(marchon.client_id)]
    
    
    
    base = base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 
    
    query = """select email email, user_fields_gestor_rentabilizacao from zendeskusers

    where 1=1
    and user_fields_gestor_rentabilizacao is not  null
    --and phone is not null
    """

    x = sqlio.read_sql_query(query, con_pipe)
    base = base.merge(x, on = 'email', how = 'left')
    base = base.dropna(subset = ['user_fields_gestor_rentabilizacao'])
    
    return base


def reserva_payload():

    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)
    l=utils.pos_count_by_client_id()
    
    #base = base[(base.total_sell > 500)]
    base = base[base.client_id.isin(l.client_id)]
    base['cs'] = 1
    
    base = base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 
    
    lista_df=pd.read_excel('CLIENTES RESERVA NA BLU_JULHO20.xlsx')
    lista_df = lista_df.drop(index = 0, axis = 1)
    lista_df = lista_df.rename({'Unnamed: 1':'cpf_cnpj'}, axis = 1)
    
    query_1 = """select email email, user_fields_gestor_rentabilizacao from zendeskusers  where 1=1
    and user_fields_gestor_rentabilizacao is not  null
    --and phone is not null
    """

    x = sqlio.read_sql_query(query_1, con_pipe)
    base = base.merge(x, on = 'email', how = 'left')

    query_2 = """select name,client_id, cpf_cnpj, distributor, seller from clients"""
    clients=sqlio.read_sql_query(query_2, con_pagnet_production)
    clients = clients[clients.cpf_cnpj.isin(lista_df.cpf_cnpj)]

    base = base[base.client_id.isin(clients.client_id)]
    base = base.dropna(subset = ['user_fields_gestor_rentabilizacao'])
    base = base.drop_duplicates('client_id')

    return base


def clientes_novos_payload():

    #base=utils.basic_client_infos()
    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    wallet_defined=pd.read_csv('wallet_defined.csv')
    base=base[~base.client_id.isin(wallet_defined.client_id)]
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)
    
    base = base[(base.churn == 0) & (base.total_sell > 500)]
    base = base[(base.days_in_blu >= 30)]
    base = base[(base.days_in_blu <= 90)]
    base = base[base.total_buy < -2]
    
    #return base
    
    base['cs'] = 1
    
    base=base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 
    df=pd.read_csv('ref.csv')
    base = base[~base.client_id.isin(df.client_id)]

    return base



def gazin_payload():

    query = """select clients.client_id, sum(gross_value) as buy_value, min(client_transactions.created_at) from client_transactions

    inner join clients on clients.id = client_transactions.client_id
    inner join departments on departments.id = clients.department_id
    inner join clients forn on forn.id = client_transactions.client_receiver_id


    where clients.store_id in (12,80,115,81,149)
    --and departments.id in (9,45,36,25,8,7,6)
    and status = 'confirmed'
    and transaction_category_id = (33)
    and nature = 'outflow'
    and forn.distributor = True
    and forn.id in (49342,
    48076,
    48078,
    49341,
    47751,
    48071,
    48075,
    48077,
    48079)
    group by clients.client_id"""


    df_buy = query_in_db(query,'pagnet_read_replica.json')
    
    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    wallet_defined=pd.read_csv('wallet_defined.csv')
    base=base[~base.client_id.isin(wallet_defined.client_id)]
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)
    pos=utils.pos_count_by_client_id()  
    
    base=base.merge(pos, on = 'client_id')
    
    base = base[(base.total_sell > 500)]
    base = base[(base.pos_count > 0) & (base.total_sell > 500)]  
    
    base['cs'] = 1
    

    
    base = base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 


    base=base[~base.client_id.isin(df_buy.client_id)]

    gavin_df=pd.read_excel('Base Blu x Base Gazin.xlsx')
    gavin_df = gavin_df[gavin_df.client_id.isin(base.client_id)]

    base = base[base.client_id.isin(gavin_df.client_id)]
    base =  base.drop_duplicates('client_id')
    return base


def boleto_blu_payload():

    query = """select pc.id,
    c.name as "cobrador", 
    c.cpf_cnpj as "cnpj_cobrador",
    c2.client_id,
    c2.name as "cobrado", 
    c2.cpf_cnpj as "cnpj_cobrado", 
    pc.kind,
    pc.status,
    pc.created_at,
    pc.scheduled_at as data_agendada,
    pc.uuid,
    pc.inputted_value valor,
    pco.kind,
    pco.tax_percentage

    from payment_collection_options pco
    join payment_collections pc on pco.payment_collection_id = pc.id
    join clients c on c.id = pc.charger_id
    join clients c2 on c2.id = pc.charged_id

    where pco.kind = 'payment_collection_blu_billet'
    and c.distributor = True
    and c.seller = False
    and c2.distributor = False
    and c2.seller = False
    and pco.created_at > '2020-06-20'
    and c2.department_id in (9,45,36, 8, 7,6,25)
    and c.cpf_cnpj <> '98193590000110'
    and c.cpf_cnpj <>'54593761000123'
    and c.cpf_cnpj <>'66776317000122'
    and c.cpf_cnpj <>'41411725000150'
    and c.cpf_cnpj <>'63321876000150'"""


    select_buys_bills = sqlio.read_sql_query(query, con_pagnet_production)
    select_buys_bills['Teve_opÃ§ao_de_Blu_Boleto?'] = 'Sim'
    
    #base=utils.basic_client_infos()
    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
  
    
    wallet_defined=pd.read_csv('wallet_defined.csv')
    base=base[~base.client_id.isin(wallet_defined.client_id)]
    
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days

    base['days_in_blu'].fillna(0, inplace =True)
    
    base = base[(base.churn == 0) & (base.total_sell > 500) & (base.time_in_churn <= 25)]
    
    base['cs'] = 1
    
    base = base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 
  
    base=base[base.client_id.isin(select_buys_bills.client_id)]
    select_buys_bills = select_buys_bills[['client_id','cobrador','cobrado', 'status', 'valor','created_at']]
    select_buys_bills['created_at'] =select_buys_bills.created_at.dt.to_period('d')

    textos=[]
    for i in select_buys_bills.client_id:
        textos.append(select_buys_bills[select_buys_bills.client_id == i].drop(['client_id','status'],axis = 1).to_html(index = False).replace('\n', '').replace('\r', '').replace('.','@').replace(',','.').replace('@',','))

    select_buys_bills['texto_boleto'] = textos
    select_buys_bills = select_buys_bills.drop_duplicates('client_id')
    base = base.merge(select_buys_bills[['client_id','texto_boleto']],on = 'client_id')
    base = base.drop_duplicates('client_id')
    
    query = """select email email, user_fields_gestor_rentabilizacao from zendeskusers

    where 1=1
    and user_fields_gestor_rentabilizacao is not  null
    --and phone is not null
    """

    x = sqlio.read_sql_query(query, con_pipe)

    base = base.merge(x, on = 'email', how = 'left')
    base = base.dropna(subset = ['user_fields_gestor_rentabilizacao'])
    
    return base


def boleto_ted_payload():
   
    import pandas as pd
    df = pd.read_csv('s3://blu-etl/alerts.csv')

    df['happened_at']=pd.to_datetime(df.happened_at)
    df=df[df.happened_at > '2020-05-16']

    import pandas as pd
    from blupy.connections.db_connections import query_in_db as query_in_db

    import blupy.subject.basic_client_infos as clients

    query = """select clients.client_id, client_receiver_id client_id_destination, min(happened_at) as first_buy, max(happened_at) as last_buy, 'True' as validation from client_transactions

    inner join clients on clients.id = client_transactions.client_id
    inner join departments on departments.id = clients.department_id
    inner join clients forn on forn.id = client_transactions.client_receiver_id


    where clients.store_id in (12,80,115,81,149)
    and departments.id in (9,45,36,8,7,6,25)
    and status = 'confirmed'
    and transaction_category_id = (33)
    and nature = 'outflow'
    and gross_value < -50
    and forn.distributor = True
    group by clients.client_id, client_receiver_id """


    df_buy = query_in_db(query,'pagnet_read_replica.json')

    df = df.merge(df_buy, on = ['client_id','client_id_destination'], how = 'left')

    df = df[df.validation.isnull()]

    query = """select client_receiver_id client_id_destination, -1 * sum(gross_value) as total_forns, min(happened_at) as first_forn, max(happened_at) as last_forn  from client_transactions

    inner join clients on clients.id = client_transactions.client_id
    inner join departments on departments.id = clients.department_id
    inner join clients forn on forn.id = client_transactions.client_receiver_id


    where clients.store_id in (12,80,115,81,149)
    and departments.id in (9,45,36,8,7,6,25)
    and status = 'confirmed'
    and transaction_category_id = (33)
    and nature = 'outflow'
    and forn.distributor = True
    group by client_receiver_id """


    df_buy_forns = query_in_db(query,'pagnet_read_replica.json')

    df = df.merge(df_buy_forns, on = 'client_id_destination', how = 'left')

    df = df[df.last_forn > '2020-04-01']
    df = df[df.total_forns > 5000]

    #base=utils.basic_client_infos()
    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    wallet_defined=pd.read_csv('wallet_defined.csv')
    base=base[~base.client_id.isin(wallet_defined.client_id)]
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)
    
    base = base[(base.churn == 0) & (base.total_sell > 500)]  
    
    base['cs'] = 1
    
    base = base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 
    base = base[base.client_id.isin(df.client_id)]
    base = base.drop_duplicates('client_id')
    
    return base




def araplac_payload():
    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)
    l=utils.pos_count_by_client_id()
    
    base = base[(base.total_sell > 500)]
    base = base[base.client_id.isin(l.client_id)]
    araplac=pd.read_csv('araplac_client_id.csv')
    base=base[base.client_id.isin(araplac.client_id)]
    base['cs'] = 1
    
    base=base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']]
    
    query = """select email email, user_fields_gestor_rentabilizacao from zendeskusers

    where 1=1
    and user_fields_gestor_rentabilizacao is not  null
    --and phone is not null
    """

    x = sqlio.read_sql_query(query, con_pipe)

    base = base.merge(x, on = 'email', how = 'left')

    base = base.dropna(subset = ['user_fields_gestor_rentabilizacao'])
    
    return base



def low_connections_payload():

    
    from blupy.connections.db_connections import query_in_db as query_in_db

    query = """select clients.client_id, count(distinct(client_receiver_id)) from client_transactions

    inner join clients on clients.id = client_transactions.client_id
    inner join departments on departments.id = clients.department_id
    inner join clients forn on forn.id = client_transactions.client_receiver_id


    where clients.store_id in (12,80,115,81,149)
    and departments.id in (9,45,36,8,7,6)
    and status = 'confirmed'
    and transaction_category_id = (33)
    and nature = 'outflow'
    and gross_value < -50
    and forn.distributor = True
    and client_transactions.created_at >= '2020-04-01'
    group by clients.client_id """


    df_buy = query_in_db(query,'pagnet_read_replica.json')

    import pandas as pd

    import blupy.subject.basic_client_infos as clients

    df = pd.read_csv('base.csv')

    df = df.merge(df_buy, on = 'client_id', how = 'left')

    df = df[df.churn == 0]
    df['first_buy']=pd.to_datetime(df.first_buy)
    df['last_buy']=pd.to_datetime(df.last_buy)

    df=df[df.first_buy <= '2020-04-01']

    df.fillna(0, inplace = True)

    df = df[df['count'] <= 1]
    df = df[df['count'] < df['n_forns']]

    df=df[df.total_buy <= -100]



    query = """select clients.client_id from clients

    where clients.department_id = 25"""

    deps = query_in_db(query,'pagnet_read_replica.json')

    df=df[~df.client_id.isin(deps.client_id)]

    
    #base=utils.basic_client_infos()
    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    wallet_defined=pd.read_csv('wallet_defined.csv')
    base=base[~base.client_id.isin(wallet_defined.client_id)]
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)    
    
    base['cs'] = 1
    
    base = base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 
    
    base=base[base.client_id.isin(df.client_id)]
    
    query = """select email email, user_fields_gestor_rentabilizacao from zendeskusers

    where 1=1
    and user_fields_gestor_rentabilizacao is not  null
    --and phone is not null
    """

    x = sqlio.read_sql_query(query, con_pipe)
    
    base = base.merge(x, on = 'email', how = 'left')
    df=pd.read_csv('ref.csv')
    base = base[~base.client_id.isin(df.client_id)]
    base = base.dropna(subset = ['user_fields_gestor_rentabilizacao'])
    return base


def forn_fall_payload():
    from blupy.connections.db_connections import query_in_db as query_in_db

    
    query = """select clients.client_id, count(distinct(client_receiver_id)), min(client_transactions.happened_at) as first_buy from client_transactions

    inner join clients on clients.id = client_transactions.client_id
    inner join departments on departments.id = clients.department_id
    inner join clients forn on forn.id = client_transactions.client_receiver_id


    where clients.store_id in (12,80,115,81,149)
    and departments.id in (9,45,36,8,7,6,5)
    and status = 'confirmed'
    and transaction_category_id = (33)
    and nature = 'outflow'
    and gross_value < -50
    and forn.distributor = True
    group by clients.client_id """


    df_buy = query_in_db(query,'pagnet_read_replica.json')

    import pandas as pd


    df_buy=df_buy[df_buy.first_buy <= '2020-04-01']
    df_buy.fillna(0, inplace = True)


    query = """select clients.client_id from clients

    where clients.department_id = 25"""

    deps = query_in_db(query,'pagnet_read_replica.json')

    df_buy=df_buy[~df_buy.client_id.isin(deps.client_id)]

    df_buy=df_buy[df_buy['count'] == 1]



    df=pd.read_csv('ref.csv')
    
    #base=utils.basic_client_infos()
    base = pd.read_csv('base.csv')
    users = utils.select_users()
    
    #wallet_defined=pd.read_csv('wallet_defined.csv')
    #base=base[~base.client_id.isin(wallet_defined.client_id)]
    
    base['loyal_date']=pd.to_datetime(base.loyal_date)
    base.total_buy.fillna(0, inplace = True)
    base['days_in_blu']=(pd.Timestamp.now() - base.loyal_date).dt.days
    base['days_in_blu'].fillna(0, inplace =True)    
    
    base['cs'] = 1
    base=base[base.churn == 0]
    base=base[base.total_sell > 500]
    base = base.merge(users, on = 'client_id')[['client_id','cs','user_id','email']] 
    
    query = """select email email, user_fields_gestor_rentabilizacao from zendeskusers

    where 1=1
    and user_fields_gestor_rentabilizacao is not  null
    --and phone is not null
    """

    x = sqlio.read_sql_query(query, con_pipe)

    base = base.merge(x, on = 'email', how = 'left')
    base = base[~base.client_id.isin(df.client_id)]
    base = base[base.client_id.isin(df_buy.client_id)]

    base = base.dropna(subset = ['user_fields_gestor_rentabilizacao'])
    
    return base