import pandas as pd
import psycopg2
import pandas.io.sql as sqlio
import numpy as np
import os,sys, inspect
import db_connections
import boto3
from datetime import datetime
from s3 import *

import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)


con_destiny = db_connections.connect('blu-rds-datascience.json')
con_pipe = db_connections.connect('blu_datascience.json')
con_pagnet_production = db_connections.connect('pagnet_read_replica.json')
#con_ds_redshift = db_connections.connect('dw_redshift.json')


def ecdf(data):
    """Compute ECDF for a one-dimensional array of measurements."""
    # Number of data points: n
    n = len(data)

    # x-data for the ECDF: x
    x = np.sort(data)

    # y-data for the ECDF: y
    y = np.arange(1, n+1) / n

    return x, y


def churn(c):
    
    if c['time_in_churn'] > 30:
        c = 1
    
    else:
        c = 0
        
    return c


def early_churn(c):
    
    if c['time_in_blu_days'] <= 90 :
        c = 1
    
    else:
        c = 0
        
    return c

def possible_buys(c):
    
    forn_direcionado=forn_interesse[forn_interesse.client_id == c.client_id]
    return forn_direcionado[forn_direcionado.mean_ticket < c.future_balance].shape[0]

def ted_bill_distributor():

    query = """select * from alerts"""

    boleto_and_ted = sqlio.read_sql_query(query, con_ds_redshift)    
    
    
    query = """select 
    distinct(c2.id) client_id_destination

    from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id
    inner join clients c2 on c2.id = ct.client_receiver_id


    where ct.transaction_category_id = 33
    and status = 'confirmed'
    and c2.distributor = True
    and c2.seller = False
    and c1.distributor = False
    and c1.seller = False
    and nature = 'outflow'
    and ct.created_at > now() - INTERVAL '90 DAY'

    group by c2.id
    
    having sum(gross_value) < -9000
    """


    boleto_and_ted_true_forns = sqlio.read_sql_query(query, con_pagnet_production)    
    boleto_and_ted = boleto_and_ted[boleto_and_ted.client_id_destination.isin(boleto_and_ted_true_forns.client_id_destination)]
    
    return boleto_and_ted


def basic_client_infos(store_id = (12,80,115,149), department_id = (9,45,36,25,5,6,7,8,9,46)):

    query = """select vendas.client_id as client_id,
    date_trunc('days',vendas.min_sell) first_sell,
    date_trunc('days',vendas.max_sell) last_sell,
    date_trunc('days',compras.min_buy) first_buy,
    date_trunc('days',compras.max_buy) last_buy,
    vendas.venda_total as total_sell,
    -1 * compras.compra_total as total_buy,
    compras.numero_de_compras n_buys,
    compras.n_fornecedores n_fors

    from


    (select c1.client_id  as client_id, 

    min(ct.happened_at) min_sell,
    max(ct.happened_at) max_sell,
    sum(gross_value) venda_total

    from client_transactions ct


    inner join clients c1 on c1.id = ct.client_id

    where ct.transaction_category_id in (92,93)
    and status = 'confirmed'
    and c1.distributor = False
    and c1.seller = False
    and c1.store_id in {stores}
    and c1.department_id in {departaments}

    group by c1.client_id) vendas

    left join 

    (select c1.client_id as client_id, 
    min(ct.happened_at) min_buy,
    max(ct.happened_at) max_buy,
    sum(gross_value) compra_total,
    count(distinct(client_receiver_id)) n_fornecedores,
    count(c1.client_id) as numero_de_compras

    from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id
    inner join clients c2 on c2.id = ct.client_receiver_id


    where ct.transaction_category_id = 33
    and status = 'confirmed'
    and c1.store_id in {stores}
    and c1.department_id in {departaments}
    and c2.distributor = True
    and c2.seller = False
    and c1.distributor = False
    and c1.seller = False
    and nature = 'outflow'

    group by c1.client_id) compras on vendas.client_id = compras.client_id""".format(departaments = department_id,stores = store_id)

    base = sqlio.read_sql_query(query, con_pagnet_production)     
    base=base[base.client_id.isin(base.client_id.dropna())]
    
    
    query = """select client_id from clients
    
    where distributor = False
    and seller = False
    and id = client_id
    """
    
    distributor_false = sqlio.read_sql_query(query, con_pagnet_production)
    
    base=base[base.client_id.isin(distributor_false.client_id)]
    
    
                       
    query = """select c1.client_id  as client_id, 
    date_trunc('days',min(ct.happened_at)) first_true_sell

    from client_transactions ct


    inner join clients c1 on c1.id = ct.client_id

    where ct.transaction_category_id in (92,93)
    and status = 'confirmed'
    and c1.distributor = False
    and c1.seller = False
    and c1.store_id in {stores}
    and c1.department_id in {departaments}
    and gross_value >= 10


    group by c1.client_id""".format(departaments = department_id,stores = store_id)

    first_true_sell = sqlio.read_sql_query(query, con_pagnet_production)            
                       
    base = base.merge(first_true_sell, on = 'client_id', how = 'left')
                   
    base['time_in_churn'] =  (pd.Timestamp.now() - base.last_sell).dt.days
    base['time_in_blu_months'] =  np.rint((base.last_sell - base.first_true_sell ).dt.days/30)
    base['time_in_blu_days'] =  np.rint((base.last_sell - base.first_true_sell ).dt.days)
    
    got_loyalty= loyalty(store_id, department_id )
    
    base=base.merge(got_loyalty, on = 'client_id', how = 'left')
    
    base['time_to_loyal']= (base.loyal_date - base.first_sell).dt.days
    base['churn']=base.apply(churn, axis= 1)
    base['earl_churn']=base.apply(early_churn, axis = 1)
    
    pipe_activate=checking_activate()

    base = base.merge(pipe_activate, on = 'client_id', how = 'left')
    
    return base

def total_bill_paid(store_id = (12,80,115), department_id = (6,7,8,9,45,36)):   
    
    query = """select c1.client_id  as client_id, 
    -1 * sum(gross_value) total_bill_paid

    from client_transactions ct


    inner join clients c1 on c1.id = ct.client_id

    where ct.transaction_category_id = 41
    and status = 'confirmed'
    and c1.distributor = False
    and c1.seller = False
    and c1.store_id in {stores}
    and c1.department_id in {departaments}


    group by c1.client_id""".format(departaments = department_id,stores = store_id)

    total_boleto_paid = sqlio.read_sql_query(query, con_pagnet_production)
    
    return total_boleto_paid 


def ted_in_out(store_id = (12,80,115), department_id = (6,7,8,9,45,36), in_or_out_client_id = 'in'):   

    ted_in_out=ted_in_out_execution(store_id, department_id, in_or_out_client_id)
    
    return ted_in_out


def ted_in_out_execution(store_id, department_id, in_or_out_client_id):   
    
    query = """select clients.id, clients.client_id ,bank_accounts.cpf_cnpj as cpf_cnpj_destino, bank_transactions.value, client_transactions.happened_at from bank_transactions
    inner join bank_accounts on bank_accounts.id = bank_transactions.bank_account_id
    inner join clients on clients.id = bank_accounts.client_id
    inner join client_transactions on bank_transactions.client_transaction_id = client_transactions.id

    where client_transactions.transaction_category_id = 94
    and client_transactions.status = 'confirmed'
    and client_transactions.nature= 'outflow'
    and clients.distributor= False
    and clients.seller=False
    and clients.kind='PJ'
    and clients.department_id IN {departaments}
    and clients.store_id in {stores} 
    """.format(stores = store_id, departaments = department_id)

    trans_bancarias = sqlio.read_sql_query(query, con_pagnet_production)



    query  = """select distinct(d.client_id), d.cpf_cnpj from (select clients.client_id, clients.cpf_cnpj,'clients' as tipo from clients
    where
     clients.distributor= False
     and clients.seller=False
     and clients.kind='PJ'
     and clients.department_id in departaments
     and store_id in stores
    union
    select cl.client_id, users.cpf, 'legal_rep' as tipo from users
    inner join user_clients on user_clients.user_id = users.id
    inner join clients cl on cl.id = user_clients.client_id
    where
     cl.distributor= False
     and cl.seller=False
     and cl.kind='PJ'
     and cl.department_id in departaments
     and c1.store_id in stores

     and user_clients.legal_representative = True) d""".format(stores = store_id, departaments = department_id)



    CNPJS = sqlio.read_sql_query(query, con_pagnet_production)

    trans_bank_others_account = CNPJS.merge(trans_bancarias, left_on = ['cpf_cnpj','client_id'], right_on = ['cpf_cnpj_destino','client_id'], how='outer', indicator=True )
    
    
    if in_or_out_client_id == 'in':
        trans_bank_others_account=trans_bank_others_account[trans_bank_others_account._merge.isin(['both'])].groupby('client_id').sum()[['value']].reset_index().rename({'value':'total_ted_other_accounts'})
    elif in_or_out_client_id == 'out':
        trans_bank_others_account=trans_bank_others_account[trans_bank_others_account._merge.isin(['right_only'])].groupby('client_id').sum()[['value']].reset_index().rename({'value':'total_ted_other_accounts'})
        
    return trans_bank_others_account


def all_balances():   

    query = """select client_id, sum(total_balance) total_balance, sum(current_balance) current_balance, sum(future_balance) future_balance from clients 

    group by client_id"""

    base_balance = sqlio.read_sql_query(query, con_pagnet_production)
    
    return base_balance


def loyalty(store_id = (12,80,115,149), department_id = (6,7,8,9,45,36,25)):  

    query = """select clients.client_id as client_id,  date_trunc('days', happened_at) date, sum(gross_value) gross_value from client_transactions

    inner join clients on clients.id = client_transactions.client_id


    where transaction_category_id in (92,93)
    and status = 'confirmed'
    and clients.department_id in {departaments}
    and clients.store_id in {stores}

    group by clients.client_id,  date_trunc('days', happened_at)""".format(stores = store_id, departaments = department_id)

    ganho = sqlio.read_sql_query(query, con_pagnet_production)

    ganho=ganho.sort_values(by = ['client_id','date']).groupby(by=['client_id','date']).sum().groupby(level=[0]).cumsum().reset_index()
    ganho=ganho[ganho.gross_value > 500].sort_values('date').drop_duplicates('client_id')
    ganho.rename(columns= {'date':'loyal_date'}, inplace = True)
    
    return ganho


def day_acess(store_id = (12,80,115,149), department_id = (6,7,8,9,45,36,25), legal_representative = (False,True)): 

    query = """select clients.client_id, count(distinct(date_trunc('days', login_activities.created_at))) acess_to_portal from login_activities

    inner join user_clients on user_clients.user_id = login_activities.user_id
    inner join clients on clients.id = user_clients.client_id

    where scope = 'user'
    and success= True
    and clients.store_id in {stores}
    and clients.department_id in {departaments}
    and legal_representative in {legal_representative_acess}

    group by clients.client_id""".format(stores = store_id, departaments = department_id, legal_representative_acess = legal_representative)

    dias_de_acesso = sqlio.read_sql_query(query, con_pagnet_production)
    
    return dias_de_acesso


def ted_in_out_v2(in_or_out_client_id = 'in', grouped = True): 


    query = """SELECT date_trunc('days',client_transactions.happened_at) AS â€œdateâ€,DE.client_id client_id,TRUNC(ABS(SUM(gross_value-net_value)), 2) ted_receita,TRUNC(ABS(SUM(gross_value)), 2) ted_volume,coalesce(PARA.client_id=DE.client_id, FALSE) mesma_rede
      FROM client_transactions
      join bank_transactions on bank_transactions.client_transaction_id=client_transactions.id
      join bank_accounts on bank_accounts.id=bank_transactions.bank_account_id
      join clients DE on DE.id=client_transactions.client_id
      left join ((SELECT clients.client_id,clients.cpf_cnpj,'clients' AS tipo
                         FROM clients
                         WHERE 1=1
                         )
                         UNION
                         (SELECT cl.client_id, users.cpf,'legal_rep' AS tipo FROM users
                         INNER JOIN user_clients ON user_clients.user_id = users.id
                         INNER JOIN clients cl ON cl.id = user_clients.client_id
                         WHERE 1=1 AND user_clients.legal_representative = True) )as PARA
    on PARA.cpf_cnpj= bank_accounts.cpf_cnpj
      WHERE client_transactions.transaction_category_id IN(94)
            AND client_transactions.status = 'confirmed'
            AND client_transactions.nature = 'outflow'
    GROUP BY  DE.client_id, â€œdateâ€,PARA.client_id"""


    ted_in_out = sqlio.read_sql_query(query, con_pagnet_production)
    
    if in_or_out_client_id == 'in':
        ted_in_out=ted_in_out[ted_in_out.mesma_rede == True]
        
        if grouped == True: 
            ted_in_out = ted_in_out.groupby('client_id').sum().reset_index()
        
        
    elif in_or_out_client_id == 'out':
        ted_in_out=ted_in_out[ted_in_out.mesma_rede == False]
        
        if grouped == True: 
            ted_in_out = ted_in_out.groupby('client_id').sum().reset_index()
    
    elif in_or_out_client_id == 'all':
    
        if grouped == True: 
            ted_in_out = ted_in_out.groupby('client_id').sum().reset_index()
        
    
    return ted_in_out
    

def mean_ticket_forn(): 
    
    query = """select charger_id, avg(payment_collection_installments.value) mean_ticket from payment_collections

    inner join payment_collection_installments on payment_collections.id = payment_collection_installments.payment_collection_id

    join clients forns on forns.id = payment_collections.charger_id

    where parent_id isnull
    and status = 'paid'
    and value > 10
    forns.distributor = True
    forns.

    group by charger_id"""


    mean_ticket_fornecedor = sqlio.read_sql_query(query, con_pagnet_production)


def redes_key_account(owner_name = '@felipe'):


    query_pipe = """select cnpj, 
                    pipeusers.name as owner_name, 
                    title from pipedeals

    inner join pipestages on pipedeals.stage_id = pipestages.id
    inner join pipepipelines on pipedeals.pipeline_id = pipepipelines.id
    inner join pipeusers on pipeusers.id = pipedeals.user_id

    where pipedeals.active = true
    and pipedeals.deleted = False
    and pipepipelines.active = true
    and pipedeals.pipeline_id = 56
    and owner_name = '{}'
    """.format(owner_name)

    pipe = sqlio.read_sql_query(query_pipe, con_pipe)


    query_clients = """ select rede.client_id, 
    cpf_cnpj as cnpj
    from clients as rede"""

    clients = sqlio.read_sql_query(query_clients, con_pagnet_production)


    df = pd.merge(pipe, clients, on ='cnpj', how='inner')

    return df


def distribute_wallet(store_id = (12,80,115), department_id = (6,7,8,9,45,36)):

    query = """select vendas.client_id as client_id,
    date_trunc('days',vendas.min_sell) first_sell,
    date_trunc('days',vendas.max_sell) last_sell,
    date_trunc('days',compras.min_buy) first_buy,
    date_trunc('days',compras.max_buy) last_buy,
    vendas.venda_total as total_sell,
    -1 * compras.compra_total as total_buy,
    compras.numero_de_compras n_buys,
    compras.n_fornecedores n_fors

    from


    (select c1.client_id  as client_id, 

    min(ct.happened_at) min_sell,
    max(ct.happened_at) max_sell,
    sum(gross_value) venda_total

    from client_transactions ct


    inner join clients c1 on c1.id = ct.client_id

    where ct.transaction_category_id in (92,93)
    and status = 'confirmed'
    and c1.distributor = False
    and c1.seller = False
    and c1.store_id in {stores}
    and c1.department_id in {departaments}
    and happened_at >= '2019/11/01'


    group by c1.client_id) vendas

    left join 

    (select c1.client_id as client_id, 
    min(ct.happened_at) min_buy,
    max(ct.happened_at) max_buy,
    sum(gross_value) compra_total,
    count(distinct(client_receiver_id)) n_fornecedores,
    count(c1.client_id) as numero_de_compras

    from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id
    inner join clients c2 on c2.id = ct.client_receiver_id


    where ct.transaction_category_id = 33
    and status = 'confirmed'
    and c1.store_id in {stores}
    and c1.department_id in {departaments}
    and c2.distributor = True
    and c2.seller = False
    and nature = 'outflow'
    and ct.created_at >= '2019/11/01'

    group by c1.client_id) compras on compras.client_id = vendas.client_id""".format(departaments = department_id,stores = store_id)

    df = sqlio.read_sql_query(query, con_pagnet_production)     
    df=df[df.client_id.isin(df.client_id.dropna())]  
    df['score_in_wallet'] = 0.4*df.total_buy + 0.6*df.total_sell
    df=df[['client_id','score_in_wallet']]
   
    return df



def checking_activate():


    query = """select data_additional_data_old_value_formatted as come_from,
    data_additional_data_new_value_formatted as go_to,
    data_log_time as ativated_at,
    owner_name as owner_name,
    pipedeals.cnpj

    from pipedeal_changes


    inner join pipedeals on pipedeal_changes.data_item_id = pipedeals.id


    where data_new_value in (490,372)
    and data_old_value = 378 """
    ativacao = sqlio.read_sql_query(query, con_pipe)
    
    
    query = """select client_id, cpf_cnpj  cnpj from clients """
    clients = sqlio.read_sql_query(query, con_pagnet_production)

    
    ativacao = ativacao.merge(clients, on = ['cnpj'])
    
    
    ativacao = ativacao.sort_values(by = 'ativated_at').drop_duplicates('client_id')
    ativacao = ativacao[['client_id','ativated_at']]
    
    
    return ativacao


def select_users():
    
    query = """select clients.client_id, user_clients.user_id, count(user_clients.user_id) from user_clients

    inner join clients on clients.id = user_clients.client_id

    where legal_representative = True
    and clients.distributor = False
    and clients.seller= False
    and clients.department_id in (6,7,8,9,45,36,25)
    and store_id in (12,80,115,149)

    group by clients.client_id, user_id
    order by count(user_clients.user_id) desc"""


    user_infos = sqlio.read_sql_query(query, con_pagnet_production)
    
    user_infos=user_infos.sort_values(by = 'count', ascending= False).drop_duplicates('client_id')
    
    query = """select id as user_id, email from users"""

    user_email = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    user_infos = user_infos.merge(user_email, on = 'user_id')
    
    user_infos = user_infos[['client_id','user_id','email']]
    
    return user_infos



def fornecedores_pipes():


    query = """SELECT
        A.cnpj as cpf_cnpj_lojista,
        pipeorganizations.cnpj cpf_cnpj_fornecedor
    FROM (
        SELECT
            id,
            cnpj,
            fornecedor_1 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_1 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_2 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_2 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_3 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_3 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_4 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_4 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_5 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_5 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_6 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_6 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_7 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_7 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_8 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_8 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_9 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_9 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_10 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_10 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_11 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_11 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_12 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_12 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_13 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_13 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_14 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_14 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_15 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_15 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_16 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_16 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_17 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_17 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_18 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_18 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_19 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_19 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_20 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_20 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_21 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_21 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_22 AS fornecedor
        FROM
                pipedeals
            WHERE
                fornecedor_22 IS NOT NULL
                AND cnpj IS NOT NULL
                AND active IS TRUE
                AND deleted IS FALSE
            UNION
            SELECT
                id,
                cnpj,
                fornecedor_23 AS fornecedor
            FROM
                pipedeals
            WHERE
                fornecedor_23 IS NOT NULL
                AND cnpj IS NOT NULL
                AND active IS TRUE
                AND deleted IS FALSE
            UNION
            SELECT
                id,
                cnpj,
                fornecedor_24 AS fornecedor
            FROM
                pipedeals
            WHERE
                fornecedor_24 IS NOT NULL
                AND cnpj IS NOT NULL
                AND active IS TRUE
                AND deleted IS FALSE
            UNION
            SELECT
                id,
                cnpj,
                fornecedor_25 AS fornecedor
            FROM
                pipedeals
            WHERE
                fornecedor_25 IS NOT NULL
                AND cnpj IS NOT NULL
                AND active IS TRUE
                AND deleted IS FALSE
            UNION
            SELECT
                id,
                cnpj,
                fornecedor_26 AS fornecedor
            FROM
                pipedeals
            WHERE
                fornecedor_26 IS NOT NULL
                AND cnpj IS NOT NULL
                AND active IS TRUE
                AND deleted IS FALSE
            UNION
            SELECT
                id,
                cnpj,
                fornecedor_27 AS fornecedor
            FROM
                pipedeals
            WHERE
                fornecedor_27 IS NOT NULL
                AND cnpj IS NOT NULL
                AND active IS TRUE
                AND deleted IS FALSE
            UNION
            SELECT
                id,
                cnpj,
                fornecedor_28 AS fornecedor
            FROM
                pipedeals
        WHERE
            fornecedor_28 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            _fornecedor_29 AS fornecedor
        FROM
            pipedeals
        WHERE
            _fornecedor_29 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE
        UNION
        SELECT
            id,
            cnpj,
            fornecedor_30 AS fornecedor
        FROM
            pipedeals
        WHERE
            fornecedor_30 IS NOT NULL
            AND cnpj IS NOT NULL
            AND active IS TRUE
            AND deleted IS FALSE) A
    JOIN pipeorganizations ON pipeorganizations.active_flag IS TRUE
        AND CAST(pipeorganizations.id AS INT) = A.fornecedor """


    forn_interesse = sqlio.read_sql_query(query, con_pipe)

    
    query = """select client_id, cpf_cnpj as cpf_cnpj_lojista from clients 

    where distributor = False
    and seller = False
    """

    clients = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    query = """select client_id as charger_id, cpf_cnpj as cpf_cnpj_fornecedor from clients 

    where distributor = True
    and seller = False"""

    distributor = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    forn_interesse=forn_interesse.merge(clients, on = 'cpf_cnpj_lojista')
    forn_interesse=forn_interesse.merge(distributor, on = 'cpf_cnpj_fornecedor')
    
    
    query = """ select charger_id, avg(payment_collection_installments.value) mean_ticket from payment_collections

    inner join payment_collection_installments on payment_collections.id = payment_collection_installments.payment_collection_id


    where parent_id isnull
    and status = 'paid'
    and value > 10


    group by charger_id """


    mean_ticket = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    forn_interesse=forn_interesse.drop_duplicates(['client_id','charger_id'])
    forn_interesse=forn_interesse.merge(mean_ticket, on = 'charger_id' )

    return forn_interesse



def basic_client_infos_conclusao_de_venda(store_id = (12,80,115,149), department_id = (9,45,36,25,5,6,7,8,9,46,47,149)):

    query = """ select c1.client_id  as client_id, 

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
    and c1.department_id in {departaments}

    group by c1.client_id """.format(departaments = department_id,stores = store_id)

    base_sells = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    query = """ select c1.client_id from clients c1
    
    where c1.store_id in {stores}
    and c1.department_id in {departaments}
    and client_id = id 
    and c1.distributor = False
    and c1.seller = False
    
    """.format(departaments = department_id,stores = store_id)
    base = sqlio.read_sql_query(query, con_pagnet_production)
    
    base=base.merge(base_sells, on = 'client_id', how = 'left')
    base.total_sell.fillna(0, inplace = True)
                       
    base['time_in_churn'] =  (pd.Timestamp.now() - base.last_sell).dt.days
    base.time_in_churn.fillna(0, inplace = True)
   
    
    got_loyalty= loyalty(store_id, department_id )
    pos=pos_count_by_client_id(store_id, department_id )
    
    base=base.merge(got_loyalty, on = 'client_id', how = 'left')
    base=base.merge(pos, on = 'client_id', how = 'left')

    base['churn']=base.apply(churn, axis= 1)
    
    pipe_activate=checking_activate()

    base = base.merge(pipe_activate, on = 'client_id', how = 'left')
    
    return base


def avg_time_diference_between_sell():
    
    query = """ WITH
    var as (select c1.client_id, date_trunc('day',happened_at) as happened_at, sum(gross_value * ct.installments) from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id


    where ct.transaction_category_id in (92,93)
        and status = 'confirmed'
        and c1.distributor = False
        and c1.seller = False
        and c1.store_id in (12,80,115)
        and c1.department_id in (9,45,36)
        and installment = 1
        and gross_value > 10
        --and happened_at > now() - interval '60' day
        and date_part('dow', happened_at) != 0

    group by c1.client_id, date_trunc('day',happened_at))

    SELECT client_id, CASE 
            WHEN COUNT(*) < 2
                THEN 0
            ELSE DATE_PART('day', date_trunc('day',MAX(happened_at))- date_trunc('day', MIN(happened_at))) /
            (COUNT(*) - 1)
            END AS avg_time_diference_between_sells
    FROM var 

    group by client_id
    having count(*) > 10
    
    """


    avg_time_diference_between_sells_df = sqlio.read_sql_query(query, con_pagnet_production)

    avg_time_diference_between_sells_df['avg_time_diference_between_sells']=np.rint(avg_time_diference_between_sells_df['avg_time_diference_between_sells'])
    avg_time_diference_between_sells_df['avg_time_diference_between_sells'].replace(0,1, inplace  = True)
    
    
    return avg_time_diference_between_sells_df


def avg_time_diference_between_buy():
    
    query = """ WITH
    var as (select c1.client_id, date_trunc('day',happened_at) as happened_at, sum(gross_value * ct.installments) from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id
    inner join clients f1 on f1.id = ct.client_receiver_id

    where ct.transaction_category_id in (33)
        and status = 'confirmed'
        and c1.distributor = False
        and c1.seller = False
        and c1.store_id in (12,80,115)
        and c1.department_id in (9,45,36)
        and installment = 1
        and f1.distributor= True
        and f1.client_id != c1.client_id

    group by c1.client_id, date_trunc('day',happened_at))

    SELECT client_id, CASE 
            WHEN COUNT(*) < 2
                THEN 0
            ELSE DATE_PART('day', date_trunc('day',MAX(happened_at))- date_trunc('day', MIN(happened_at))) /
            (COUNT(*) - 1)
            END AS avg_time_diference_between_buys
    FROM var 

    group by client_id
    having count(*) > 3
    
    """


    avg_time_diference_between_buy = sqlio.read_sql_query(query, con_pagnet_production)
  
    return avg_time_diference_between_buy




def pos_count_by_client_id(store_id = (12,80,115, 149), department_id = (9,45,36,25,5,6,7,8,9,46,47,149)):
    
    
    query = """select 

    clients.client_id client_id, 
    count(*) as pos_count
    from pos

    inner join clients on clients.id = pos.client_id
    group by clients.client_id"""


    pos = sqlio.read_sql_query(query, con_pagnet_production)
    return pos



def avg_time_between_buy_and_pending_charges(store_id = (12,80,115,149), department_id = (9,45,36,25,5,6,7,8,9,46,47)):

    query = """ select charge_distance.*, quantidade_de_cobranÃ§as_pendentes_last_3_month, mÃ©dia_das_cobrancas_pendentes from 

    (WITH
        var as 
            (select 
            c1.client_id, 
            date_trunc('day',happened_at) as happened_at, 
            -1*(sum(gross_value * ct.installments)) as gross_value
            from client_transactions ct
    
            inner join clients c1 on c1.id = ct.client_id
            inner join clients forn on forn.id = ct.client_receiver_id
            
            where ct.transaction_category_id = 33
            and nature = 'outflow'
            and status = 'confirmed'
            
            and c1.distributor = False
            and c1.seller = False
            
            and forn.distributor = True
            
            and ct.client_id != forn.client_id
            
            and c1.store_id in (12,80,115)
            and c1.department_id in (9,45,36)
            and gross_value < -5
            and date_part('dow', happened_at) != 0
            
            group by c1.client_id, date_trunc('day',happened_at)
            )
            
        SELECT client_id, 
        CASE 
            WHEN COUNT(*) < 2 THEN 0
            ELSE DATE_PART('day', date_trunc('day',MAX(happened_at))- date_trunc('day', MIN(happened_at))) / (COUNT(*) - 1)
            END AS avg_time_diference_between_charge,
        CASE 
            WHEN COUNT(*) < 2 THEN 0
            ELSE  sum(gross_value)/ (COUNT(*))
            END AS avg_value_charge
        FROM var 
        group by client_id
        having count(*) >= 3
    ) as charge_distance
        
    left join
        
    (select  
    
        tudÃ£o.client_id, 
        count (tudÃ£o.client_id) quantidade_de_cobranÃ§as_pendentes_last_3_month,
        sum(tudÃ£o.gross_value)/count (tudÃ£o.client_id) as mÃ©dia_das_cobrancas_pendentes
        
        from 
            (select clients.client_id, 
            payment_collection_installments.value as gross_value, 
            payment_collection_installments.payment_collection_id, 
            payment_collections.charger_id, 
            payment_collections.charged_id, 
            date_trunc ('day', payment_collections.created_at) as data, 
            'aguardando_aprovacao' as tipo  from payment_collections  
            
            inner join clients  on clients.id = payment_collections.charged_id 
            inner join payment_collection_installments on payment_collection_installments.payment_collection_id = payment_collections.id  
            
            where status = 'pending'
            and payment_collections.created_at > now() - interval '2' month
            and payment_collection_installments.value > 10) as tudÃ£o  
            
        inner join clients  on tudÃ£o.client_id = clients.id
        group by tudÃ£o.client_id) as pending_charges
    
on charge_distance.client_id = pending_charges.client_id """

    base_pending_and_avg_time_between_buys = sqlio.read_sql_query(query, con_pagnet_production)
    
    return base_pending_and_avg_time_between_buys


def pos_count_by_id():
    
    
    query = """select 
    
    clients.client_id client_id,
    clients.id,
    clients.business_name,
    clients.cpf_cnpj as cnpj,
    terminal_number as numero_do_terminal,
    model as modelo,
    property_number as numero_de_patrimonio
    
    
    from pos

    right join clients on clients.id = pos.client_id"""

    pos = sqlio.read_sql_query(query, con_pagnet_production)
    
    query = """select clients.id, pos_rent_amount preÃ§o_de_aluguel from clients

    inner join tax_plans on clients.tax_plan_id = tax_plans.id
    """

    pos_price = sqlio.read_sql_query(query, con_pagnet_production)
    
    pos = pos.merge(pos_price, on = 'id', how = 'left')
    
    return pos



def cnpj_level_breaker():
    
    
    query = """select 
    
    clients.client_id client_id,
    clients.id,
    clients.business_name nome_fantasia,
    clients.cpf_cnpj as cnpj
    
    from clients
    
    where clients.store_id in (12,80,115)"""
    
    cpnj_level = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    query = """select 
    client_transactions.client_id id,
    max(client_transactions.happened_at) ultima_venda,
    min(client_transactions.happened_at) primeira_venda
    
    from client_transactions
    
    where client_transactions.transaction_category_id in (92,93)
    and client_transactions.status = 'confirmed'
    and nature = 'inflow'
    
    
    group by client_transactions.client_id"""
    last_and_first_transactions = sqlio.read_sql_query(query, con_pagnet_production)
    
    cpnj_level = cpnj_level.merge(last_and_first_transactions, on = 'id')
    
    query = """select id, current_balance saldo_atual, future_balance saldo_futuro, total_balance saldo_total from clients
    where store_id in (12,80,115)"""
    
    balances = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    cpnj_level = cpnj_level.merge(balances, on = 'id')
    
    return cpnj_level





def pzo_medio():


    query = """select x.client_id, sum(x.numerador)/ sum(x.gross_value) as prazo_medio_de_venda from 

    (select
    clients.client_id as client_id,
    client_transactions.client_id as id,
    gross_value,
    happened_at,
    provider_released_at,
    ((client_transactions.provider_released_at at time zone 'utc' at time zone 'brt')::date - (client_transactions.happened_at at time zone 'utc' at time zone 'brt')::date) as prazo,
    gross_value * ((client_transactions.provider_released_at at time zone 'utc' at time zone 'brt')::date - (client_transactions.happened_at at time zone 'utc' at time zone 'brt')::date)  as numerador
    from client_transactions

    inner join clients on clients.id = client_transactions.client_id

    where transaction_category_id = 93
    and status = 'confirmed'
    and gross_value > 10
    and seller = False
    and distributor = False
    order by client_transactions.client_id) x
    group by client_id """



    pzo_med = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    return pzo_med


def tax_plan():


    query = """select 
    clients.id client_id,
    debit_rate,
    credit_rate_1,
    credit_rate_2 credito_2_6x,
    credit_rate_7 credito_7_12x,
    tax_plans.anticipation_rate
    
    from tax_plans
    inner join clients on clients.tax_plan_id = tax_plans.id"""

    tax_plan_id = sqlio.read_sql_query(query, con_pagnet_production)
    
    
    return tax_plan_id




def antecipation_value_by_client_id():



    query = """select clients.client_id, -1 * sum(advances_rate_value) as taxa_paga, sum(advanced_value) as valor_bruto_antecipado from client_transactions

    inner join clients on clients.id = client_transactions.client_id

    where status = 'confirmed'
    and advances_rate_value < 0 
    and advanced_value > 0
    and transaction_category_id != 33

    group by clients.client_id"""

    antecipation_value = sqlio.read_sql_query(query, con_pagnet_production)
    
    return antecipation_value