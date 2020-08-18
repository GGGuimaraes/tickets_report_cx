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


def churn(c):
    
    if c['time_in_churn'] > 30:
        c = 1
    
    else:
        c = 0
        
    return c


def early_churn(c):
    
    if c['time_in_blu_days'] <= 60 & c['time_in_churn'] > 30 :
        c = 1
    
    else:
        c = 0
        
    return c


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
    and c1.department_id in {departaments}

    group by c1.client_id""".format(departaments = department_id,stores = store_id)

    base = query_in_db(query, 'pagnet_read_replica.json')     


    query = """select c1.client_id as client_id, 
    min(ct.happened_at) first_buy,
    max(ct.happened_at) last_buy,
    sum(gross_value) total_buy,
    count(distinct(client_receiver_id)) n_forns,
    count(c1.client_id) as n_buys

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

    group by c1.client_id""".format(departaments = department_id,stores = store_id)

    buys = query_in_db(query, 'pagnet_read_replica.json')     
    
    base = base.merge(buys, on = 'client_id', how = 'left')
    
                       
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
    first_true_sell = query_in_db(query, 'pagnet_read_replica.json')            
                       
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



def loyalty(store_id = (12,80,115), department_id = (6,7,8,9,45,36)):  

    query = """select clients.client_id as client_id,  date_trunc('days', happened_at) date, sum(gross_value) gross_value from client_transactions

    inner join clients on clients.id = client_transactions.client_id


    where transaction_category_id in (92,93)
    and status = 'confirmed'
    and clients.department_id in {departaments}
    and clients.store_id in {stores}

    group by clients.client_id,  date_trunc('days', happened_at)""".format(stores = store_id, departaments = department_id)

    ganho = query_in_db(query, 'pagnet_read_replica.json')

    ganho=ganho.sort_values(by = ['client_id','date']).groupby(by=['client_id','date']).sum().groupby(level=[0]).cumsum().reset_index()
    ganho=ganho[ganho.gross_value > 500].sort_values('date').drop_duplicates('client_id')
    ganho.rename(columns= {'date':'loyal_date'}, inplace = True)
    
    return ganho[['client_id','loyal_date']]

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
    ativacao = query_in_db(query, 'blu_datascience.json')
    
    
    query = """select client_id, cpf_cnpj  cnpj from clients """
    clients = query_in_db(query, 'pagnet_read_replica.json')

    
    ativacao = ativacao.merge(clients, on = ['cnpj'])
    
    
    ativacao = ativacao.sort_values(by = 'ativated_at').drop_duplicates('client_id')
    ativacao = ativacao[['client_id','ativated_at']]
    
    
    return ativacao


# In[3]:


base = basic_client_infos(store_id = (12,80,115,149), department_id = (9,45,36,25,5,6,7,8,9,46,47,149))


# In[4]:


#base = clients(store_id = (12,80,115,149), department_id = (9,45,36,25,5,6,7,8,9,46,47,149))
base = base[base.client_id != 25997]
base = base[base.client_id != 38983]
base.to_csv('base.csv', index = False)


# In[5]:


query = """select email, phone telefone_1, phone_2 telefon_2, phone_3 telefone_3 from users"""

users_df = query_in_db(query,  'pagnet_read_replica.json')

def users(users):
    d = []
    for p in users.email.unique():
        d.append(
            {
                'email': p,
                'numbers': users[users.email == p].drop(['email'], axis = 1).to_html(index = False).replace('\n', '').replace('\r', '')
            }
        )

    users_df = pd.DataFrame(d)
    users_df['numbers'] ="""<p><strong>Números: </strong></p>""" + users_df.numbers
        
    return users_df


user_df = users(users_df)

user_df.to_csv('users_tel.csv', index = False)


# In[6]:


def user_selection():
    
    query = """select clients.client_id, user_clients.user_id, count(user_clients.user_id)  from user_clients

    inner join clients on clients.id = user_clients.client_id

    where legal_representative = True
    and clients.distributor = False
    and clients.seller= False
    and store_id in (12,80,115,149)
    and clients.department_id in (6,7,8,9,45,36,25)

    group by clients.client_id, user_id

    order by count(user_clients.user_id) desc"""


    user_infos = query_in_db(query,  'pagnet_read_replica.json')
    user_infos=user_infos.sort_values(by = 'count', ascending= False).drop_duplicates('client_id')
    
    
    query = """select id as user_id, name name_usuario from users """
    usuarios_nome = query_in_db(query,  'pagnet_read_replica.json')
    
    
    user_infos=user_infos.merge(usuarios_nome, on='user_id' ,how = 'inner' ).fillna(0)
    
    return user_infos


# In[7]:


def main_pos_info():
    
    query = """select 

        clients.id, 
        count(*) as POS_alocadas

    from pos

    inner join clients on clients.id = pos.client_id

    group by clients.id"""

    pos = query_in_db(query,  'pagnet_read_replica.json')
    
    
    query = """select id,
    client_id, 
    cpf_cnpj as CNPJ,
    total_balance saldo_total,
    current_balance saldo_atual,
    future_balance saldo_futuro from clients """
    clients_balance = query_in_db(query,  'pagnet_read_replica.json')
    
    
    clients_balance=clients_balance.merge(pos, on='id',how = 'left' ).fillna(0)
    
    query = """select 
    client_id as id,max(happened_at) ultima_venda,
    min(happened_at) primeira_venda from client_transactions
    
    where transaction_category_id in (92,93)
    and status = 'confirmed'
    
    group by client_transactions.client_id
    """
    last_sell = query_in_db(query,  'pagnet_read_replica.json')
    
    clients_balance=clients_balance.merge(last_sell, on='id',how = 'left')

    
    return clients_balance


# In[8]:


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



    pzo_med = query_in_db(query,  'pagnet_read_replica.json')

    return pzo_med


# In[9]:


def tax_plan():

    query = """select 
        clients.id client_id,
        debit_rate,
        credit_rate_1,
        credit_rate_2,
        credit_rate_7,
        tax_plans.anticipation_rate

    from tax_plans


    inner join clients on clients.tax_plan_id = tax_plans.id"""


    txa = query_in_db(query,  'pagnet_read_replica.json')
    
    return txa


# In[10]:


def antecipation():

    query = """select clients.client_id, -1 * sum(advances_rate_value) as taxa_paga, sum(advanced_value) as valor_bruto_antecipado from client_transactions

    inner join clients on clients.id = client_transactions.client_id

    where status = 'confirmed'
    and advances_rate_value < 0 
    and advanced_value > 0
    and transaction_category_id != 33



    group by clients.client_id"""


    valor_antecipado = query_in_db(query,  'pagnet_read_replica.json')

    return valor_antecipado

    


# In[11]:


def ted_boleto():
    
    bill_paid = pd.read_csv('s3://blu-etl/alerts.csv')
    bill_paid=bill_paid.groupby(['client_id','business_name_origin','business_name_destination','type','happened_at'], as_index = False).sum()
    bill_paid = bill_paid.rename(columns= {'value':'Ted/boleto_fornecedor', 'client_id':'client_id','business_name_origin':'Lojista' ,'business_name_destination':'Nome do Fornecedor', 'happened_at': 'data','type':'tipo'})
    return bill_paid


# In[12]:


def forns():
    
    query = """select c1.client_id as client_id, 
    c2.business_name as nome_fornecedor,
    c2.cpf_cnpj as cpf_cnpj_fornecedor,
    c2.client_id as charger_id,
    1 * round(sum(gross_value)) compra_total,
    max(happened_at) as ultima_compra

    from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id
    inner join clients c2 on c2.id = ct.client_receiver_id


    where ct.transaction_category_id = 33
    and status = 'confirmed'
    and c1.store_id in (12,80,115,149)
    and c2.distributor = True
    and c2.seller = False
    and nature = 'outflow'

    group by c1.client_id,c2.business_name, c2.cpf_cnpj, c2.client_id"""

    compras_fornecedor = query_in_db(query,  'pagnet_read_replica.json')


    query = """select clients.client_id,
    payment_collections.created_at, 
    charger_id, 
    value valor_ultima_compra

    from payment_collections 
    inner join payment_collection_installments on payment_collections.id = payment_collection_installments.payment_collection_id
    inner join clients on clients.id = payment_collections.charged_id
    inner join clients forns on forns.id = payment_collections.charger_id

    where status = 'paid'
    and parent_id is null
    and forns.distributor = True
    and clients.distributor = False


    order by payment_collections.created_at asc"""

    df_buys = query_in_db(query,'pagnet_read_replica.json')
    df_buys_last_buy = df_buys.sort_values('created_at').drop_duplicates(['client_id','charger_id'], keep = 'last')


    compras_fornecedor = compras_fornecedor.merge(df_buys_last_buy, on = ['client_id','charger_id'], how ='left')

    compras_fornecedor.drop(['charger_id','created_at'], axis = 1, inplace = True)
        
    
    query = """select
    cnpj cpf_cnpj_fornecedor
    ,pofo.option_label as tabela
    from pipeorganizations po
    join pipeorganization_fields_options pofo
    on po.tabela_blu = pofo.option_id"""

    tabela= query_in_db(query, 'blu_datascience.json')
    
    compras_fornecedor['compra_total'] =-1 * compras_fornecedor.compra_total
    compras_fornecedor=compras_fornecedor.merge(tabela,on = 'cpf_cnpj_fornecedor',how = 'left')
    
    return compras_fornecedor


# In[13]:


def sells():
    
    query = """select c1.client_id as client_id, 
    date_trunc('months',happened_at) as data,
    round(sum(gross_value)) vendas

    from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id


    where ct.transaction_category_id in (92,93)
    and status = 'confirmed'
    and c1.store_id in (12,80,115,149)

    group by c1.client_id,c1.cpf_cnpj,date_trunc('months',happened_at) """

    vendas = query_in_db(query,  'pagnet_read_replica.json')
    
    return vendas

def buys():
    
    query = """select c1.client_id as client_id,
    date_trunc('months',ct. created_at) as data,
    -1 * round(sum(gross_value)) compras

    from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id
    inner join clients c2 on c2.id = ct.client_receiver_id


    where ct.transaction_category_id = 33
    and status = 'confirmed'
    and c1.store_id in (12,80,115,149)
    and c2.distributor = True
    and c2.seller = False
    and nature = 'outflow'


    group by c1.client_id,date_trunc('months', ct.created_at) """

    compras = query_in_db(query,  'pagnet_read_replica.json')

    return compras


def propotion(vendas,compra):
    
    vendas_compras=vendas.merge(compra, on = ['client_id','data'], how = 'outer').fillna(0)
    vendas_compras['taxa_de_compra'] = 100 * vendas_compras.compras / vendas_compras.vendas
    vendas_compras.fillna(0, inplace = True)
    vendas_compras['taxa_de_compra'] = vendas_compras.taxa_de_compra.replace([np.inf], 1000) 
    vendas_compras[['taxa_de_compra']] = vendas_compras[['taxa_de_compra']].astype('int64', copy=False)
    vendas_compras['taxa_de_compra']=vendas_compras.taxa_de_compra.astype(str) + '%'
    vendas_compras['Porcentagem de compra em relação a venda (%)'] = vendas_compras.taxa_de_compra.replace('1000%', 'só comprou esse mês')
    vendas_compras.drop('taxa_de_compra' ,axis = 1, inplace = True)
    
    return vendas_compras


# In[14]:


def sell_30_days():
    
    query = """select c1.id as id, 
    round(sum(gross_value)) venda_ultimos_30_dias

    from client_transactions ct

    inner join clients c1 on c1.id = ct.client_id


    where ct.transaction_category_id in (92,93)
    and status = 'confirmed'
    and c1.store_id in (12,80,115,149)
    and happened_at > '2020-06-12'
    group by c1.id """

    vendas = query_in_db(query,  'pagnet_read_replica.json')
    
    return vendas


# In[15]:


users = user_selection()


# In[16]:


pos=main_pos_info()


# In[17]:


pzo_medio=pzo_medio()


# In[18]:


tax=tax_plan()


# In[19]:


antecipation=antecipation()


# In[20]:


ted_boleto=ted_boleto() 


# In[21]:


forn=forns()


# In[22]:


sell=sells()


# In[23]:


buy=buys()


# In[24]:


propotion=propotion(sell, buy)


# In[25]:


sell_30=sell_30_days()


# In[26]:


#######################


# In[27]:


pos[['saldo_total','saldo_atual','saldo_futuro']] = pos[['saldo_total','saldo_atual','saldo_futuro']].astype('int64', copy=False)
pos = pos.merge(sell_30, on = 'id', how = 'left')
pos['ultima_venda'] = pos.ultima_venda.dt.strftime('%d/%m/%Y')
pos['primeira_venda'] = pos.primeira_venda.dt.strftime('%d/%m/%Y')
pos['pos_alocadas'] = pos[['pos_alocadas']].astype('int64', copy=False)
pos[['saldo_total','saldo_atual','saldo_futuro']] = pos[['saldo_total','saldo_atual','saldo_futuro']].applymap(lambda x:format_decimal(x, format='#,##0.##;-#', locale='pt'))

pos.fillna('-', inplace = True)


# In[ ]:


#ted_boleto[['Ted/boleto_fornecedor']] = ted_boleto[['Ted/boleto_fornecedor']].astype('int64', copy=False)
ted_boleto['data'] = pd.to_datetime(ted_boleto['data'])
ted_boleto['data'] = ted_boleto.data.dt.strftime('%d/%m/%Y')
ted_boleto[['Valor']] = ted_boleto[['Ted/boleto_fornecedor']].applymap(lambda x:format_decimal(x, format='#,##0.##;-#', locale='pt'))
ted_boleto = ted_boleto[['client_id','client_id_destination','Lojista','Nome do Fornecedor','tipo','data','Valor']]
ted_boleto.fillna('-', inplace = True)


# In[ ]:


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


# In[ ]:


df['aux']=df.client_id.astype(str) + df.client_id_destination.astype(str)
ted_boleto['aux'] = ted_boleto.client_id.astype(str) + ted_boleto.client_id_destination.astype(str)


# In[ ]:


ted_boleto = ted_boleto[ted_boleto.aux.isin(df.aux)]
ted_boleto = ted_boleto.drop('aux', axis = 1)


# In[ ]:


tax = tax.rename({'debit_rate':'taxa_de_debito', 'credit_rate_1':'taxa_de_credito_1x','credit_rate_2':'taxa_de_credito_2-6x', 'credit_rate_7':'taxa_de_credito_7-12x', 'anticipation_rate':'taxa_de_antecipação'}, axis = 1)


# In[ ]:


pzo_medio.fillna(0, inplace = True)
pzo_medio[['prazo_medio_de_venda']] = pzo_medio[['prazo_medio_de_venda']].astype('int64', copy=False)
pzo_medio['número_médio_de_parcelas'] = np.rint(pzo_medio['prazo_medio_de_venda']/15 -1)

pzo_medio = pzo_medio.merge(tax[['client_id','taxa_de_antecipação']], on = 'client_id', how = 'left')
pzo_medio = pzo_medio.merge(base[['client_id', 'total_buy']].fillna(0), on = 'client_id' )

pzo_medio['daily_tax'] =  (1+pzo_medio['taxa_de_antecipação'] /100)**(1 / 30) - 1
pzo_medio['period_simulated_tax'] = (1 + pzo_medio.daily_tax )**(pzo_medio.prazo_medio_de_venda) - 1
pzo_medio['economia_com_a_blu'] = 1*pzo_medio.period_simulated_tax * pzo_medio.total_buy
pzo_medio = pzo_medio[['client_id','prazo_medio_de_venda','número_médio_de_parcelas','economia_com_a_blu']]


pzo_medio[['economia_com_a_blu']] = pzo_medio[['economia_com_a_blu']].astype('int64', copy=False)
pzo_medio[['economia_com_a_blu']] = pzo_medio[['economia_com_a_blu']].applymap(lambda x:format_decimal(x, format='#,##0.##;-#', locale='pt'))


# In[ ]:


antecipation[['taxa_paga','valor_bruto_antecipado']] = antecipation[['taxa_paga','valor_bruto_antecipado']].astype('int64', copy=False)
antecipation[['valor_bruto_antecipado']] = antecipation[['valor_bruto_antecipado']].applymap(lambda x:format_decimal(x,  format='#,##0.##;-#', locale='pt'))


# In[ ]:


propotion['data'] = propotion.data.dt.strftime('%d/%m/%Y')
propotion[['vendas','compras']] = propotion[['vendas','compras']].applymap(lambda x:format_decimal(x,  format='#,##0.##;-#', locale='pt'))


# In[ ]:


forn['ultima_compra'] = forn.ultima_compra.dt.strftime('%d/%m/%Y')
forn[['compra_total']] = forn[['compra_total']].astype('int64', copy=False)
forn[['compra_total','valor_ultima_compra']] = forn[['compra_total','valor_ultima_compra']].applymap(lambda x:format_decimal(x,  format='#,##0.##;-#', locale='pt'))
forn=forn.fillna('-----')


# In[ ]:


########################


# In[ ]:


def text_pos(pos):
    d = []
    for p in pos.client_id.unique():
        d.append(
            {
                'client_id': p,
                'txt_pos': pos[pos.client_id == p].drop(['id','client_id'], axis = 1).to_html(index  = False).replace('\n', '').replace('\r', '')
            }
        )

    pos = pd.DataFrame(d)
    pos['txt_pos'] ="""<p><strong>Informações Gerais: </strong></p>""" + pos.txt_pos
        
    return pos


# In[ ]:


def text_tax(tax):
    d = []
    for p in tax.client_id.unique():
        d.append(
            {
                'client_id': p,
                'txt_tax': tax[tax.client_id == p].drop(['client_id'], axis = 1).to_html(index = False).replace('\n', '').replace('\r', '')
            }
        )

    tax = pd.DataFrame(d)
    tax['txt_tax'] ="""<p><strong>Plano de taxas: </strong></p>""" + tax.txt_tax
        
    return tax


# In[ ]:


def text_propotion(propotion):
    d = []
    for p in propotion.client_id.unique():
        d.append(
            {
                'client_id': p,
                'txt_propotion': propotion[propotion.client_id == p].sort_values('data').drop(['client_id'], axis = 1).to_html(index = False).replace('\n', '').replace('\r', '')
            }
        )

    propotion = pd.DataFrame(d)
    propotion['txt_propotion'] ="""<p><strong>Vendas, compras e relação entre venda e Compras:</strong></p>""" + propotion.txt_propotion
    return propotion


# In[ ]:


def text_forns(forns):
    d = []
    for p in forns.client_id.unique():
        d.append(
            {
                'client_id': p,
                'txt_forns': forns[forns.client_id == p].drop(['client_id'], axis = 1).to_html(index = False).replace('\n', '').replace('\r', '')
            }
        )

    forns = pd.DataFrame(d)
    forns['txt_forns'] ="""<p><strong>Fornecedores que o cliente comprou</strong></p>""" + forns.txt_forns
    
    return forns


# In[ ]:


def pzo_medio_text(pzo_medio):
    d = []
    for p in pzo_medio.client_id.unique():
        d.append(
            {
                'client_id': p,
                'txt_pzo_medio': pzo_medio[pzo_medio.client_id == p].drop(['client_id'], axis = 1).to_html(index = False).replace('\n', '').replace('\r', '')
            }
        )

    pzo_medio = pd.DataFrame(d)
    pzo_medio['txt_pzo_medio'] = """<p><strong>Prazo M&eacute;dio</strong></p>""" + pzo_medio.txt_pzo_medio
    return pzo_medio


# In[ ]:


def antecipation_text(antecipation):
    d = []
    for p in antecipation.client_id.unique():
        d.append(
            {
                'client_id': p,
                'txt_antecipation': antecipation[antecipation.client_id == p].drop(['client_id'], axis = 1).to_html(index = False).replace('\n', '').replace('\r', '')
            }
        )

    antecipation = pd.DataFrame(d)
    antecipation['txt_antecipation'] = """<p><strong>Valor antecipado total</strong></p>""" + antecipation.txt_antecipation
    return antecipation


# In[ ]:


def ted_bill(ted_boleto):
    d = []
    for p in ted_boleto.client_id.unique():
        d.append(
            {
                'client_id': p,
                'ted_boleto_text': ted_boleto[ted_boleto.client_id == p].drop(['client_id', 'client_id_destination'], axis = 1).to_html(index = False).replace('\n', '').replace('\r', '')
            }
        )

    ted_bill = pd.DataFrame(d)
    ted_bill['ted_boleto_text'] = """<p><strong>Valor antecipado total</strong></p>""" + ted_bill.ted_boleto_text
    return ted_bill


# In[ ]:


x = ted_bill(ted_boleto)


# In[ ]:


pos_text=text_pos(pos)
forns_text = text_forns(forn)
propotion_text = text_propotion(propotion)
tax_text = text_tax(tax)
ted_bill_text = ted_bill(ted_boleto)
pzo_medio = pzo_medio_text(pzo_medio)
antecipation_text_df = antecipation_text(antecipation)


# In[ ]:


#############################


# In[ ]:


pos_text = pos_text.merge(forns_text, on  = 'client_id', how = 'left')
pos_text = pos_text.merge(propotion_text, on  = 'client_id', how = 'left')
pos_text = pos_text.merge(tax_text, on  = 'client_id', how = 'left')
pos_text = pos_text.merge(pzo_medio, on  = 'client_id', how = 'left')
pos_text = pos_text.merge(antecipation_text_df, on  = 'client_id', how = 'left')
pos_text = pos_text.merge(ted_bill_text, on  = 'client_id', how = 'left')

pos_text.fillna('', inplace = True)


# In[ ]:


###########################


# In[ ]:


pos_text['text'] = pos_text[['txt_pos', 'txt_forns', 'txt_propotion', 'txt_tax','txt_pzo_medio','txt_antecipation','ted_boleto_text']].apply(lambda x: '<p><br></p><p><br></p>'.join(x), axis=1)


# In[ ]:


query = """select clients.id as client_id, 
clients.business_name as rede, 
departments.name as departamento

from clients


inner join departments on departments.id = clients.department_id


where clients.id = client_id"""

dep = query_in_db(query,  'pagnet_read_replica.json')


# In[ ]:


pos_text=pos_text.merge(dep, on = 'client_id')


# In[ ]:


pos_text['rede']=pos_text['rede']+ ' -> '+ pos_text['departamento'] + '<p><br></p>'


# In[ ]:


pos_text['text']=pos_text['rede'] + pos_text['text']


# In[ ]:


pos_text.to_csv('cliente_score/client_score_bb/codes/text_as_html.csv', index = False)


# In[ ]:





