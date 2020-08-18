import pandas as pd
import boto3
import psycopg2
import pandas.io.sql as sqlio
import numpy as np
import json
import os,sys, inspect
import db_connections
from datetime import datetime
from s3 import *
import utils
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



def avoid_contat_before_need(data, cx_destiantion_group): 
    
    #Make sure that we are not going to open tickets before the data resettled by the client
    
    data_to_call='2020-0' + str(pd.Timestamp.now().month) + '-' + str(pd.Timestamp.now().day)

    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_data_para_novo_contato from zendesktickets
    inner join zendeskusers on zendesktickets.assignee_id = zendeskusers.id
    where 1= 1
    and ticket_form_id = '{ticket_forn_id}'
    and custom_fields_data_para_novo_contato is not null
    """.format(ticket_forn_id=cx_destiantion_group)
    clients_with_scheduled_calls = sqlio.read_sql_query(query, con_pipe)
    clients_with_scheduled_calls['id_zendesk_ticket'] = clients_with_scheduled_calls.id_zendesk_ticket.astype('int')


    query = """select client_id, id_zendesk_ticket from zendesk_integration
    where created_at > '2020/03/19'
    and formulario_id = '{ticket_forn_id}'""".format(ticket_forn_id=cx_destiantion_group)
    opened_tickets = sqlio.read_sql_query(query, con_destiny)

    
    clients_with_scheduled_calls = clients_with_scheduled_calls.merge(opened_tickets, on = 'id_zendesk_ticket')

    
    
    clients_with_scheduled_calls = clients_with_scheduled_calls[clients_with_scheduled_calls.custom_fields_data_para_novo_contato > data_to_call]
    data = data[~(data.client_id.isin(clients_with_scheduled_calls.client_id))]

    return data


def create_ticket_in_scheduled_day(data, cx_destiantion_group): 
    
    #make sure that the next touchpoint will happens on the setted day 
    
    data_to_call='2020-0' + str(pd.Timestamp.now().month) + '-' + str(pd.Timestamp.now().day)

    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_data_para_novo_contato,  date_trunc('days', GETDATE() ) from zendesktickets
    inner join zendeskusers on zendesktickets.assignee_id = zendeskusers.id
    where
    custom_fields_data_para_novo_contato = '{data_to_recall}'
    and ticket_form_id = '{ticket_forn_id}'
    """.format(data_to_recall = data_to_call,ticket_forn_id = cx_destiantion_group )
    priority_call_today = sqlio.read_sql_query(query, con_pipe)
    priority_call_today['id_zendesk_ticket'] = priority_call_today.id_zendesk_ticket.astype('int')


    query = """select client_id, created_at, id_zendesk_ticket from zendesk_integration
    where created_at > '2020/03/18'"""
    opened_tickets = sqlio.read_sql_query(query, con_destiny)


    priority_call_today = priority_call_today.merge(opened_tickets, on = 'id_zendesk_ticket')


    data['cs']=np.where((data.client_id.isin(priority_call_today.client_id)), data.cs + 9999, data.cs)
    
    
    ### insert 
    
    ## remove 
    return data[~data.client_id.isin(priority_call_today.client_id)]

def past_tickets(data, days = 20):

    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_consegui_contato_, zendeskusers.email, zendesktickets.created_at from zendesktickets
    
    join zendeskusers on zendeskusers.id = zendesktickets.requester_id
    
    where (custom_fields_consegui_contato_ = 'sim__consegui_contato_com_os_números'
    or custom_fields_conseguiu_falar_com_o_cliente_ = 'sim__consegui_contato_com_os_números'
    or custom_fields_conseguiu_falar_com_o_cliente_ = 'novo_contato_agendado') """
    ticket_not_talked = sqlio.read_sql_query(query, con_pipe)
    ticket_not_talked['id_zendesk_ticket'] = ticket_not_talked.id_zendesk_ticket.astype('int')


    ticket_not_talked['created_at_past']=(pd.Timestamp.now() - ticket_not_talked.created_at).dt.days.fillna(9999)
    
    ticket_not_talked = ticket_not_talked.sort_values('created_at_past',ascending= True).drop_duplicates('email', keep = 'first')

    ticket_not_talked=ticket_not_talked[ticket_not_talked.created_at_past < days]

    
    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_consegui_contato_, zendeskusers.email, zendesktickets.created_at from zendesktickets
    
    join zendeskusers on zendeskusers.id = zendesktickets.requester_id
    
    where custom_fields_conseguiu_falar_com_o_cliente_ = 'novo_contato_agendado' 
    and zendesktickets.created_at > '2020-04-01'"""
    ticket_avoid = sqlio.read_sql_query(query, con_pipe)    
    
    
    
    
    data=data[~data.email.isin(ticket_not_talked.email)]
    data=data[~data.email.isin(ticket_avoid.email)]

    return data




def already_talked_about_it(data, subject):
    #### Revisitar ######
    
    query = """select client_id, created_at created_at_past, id_zendesk_ticket from zendesk_integration

    where created_at > '2020/01/09'
    and  subject = '{0}'""".format(subject)

    opened_tickets = sqlio.read_sql_query(query, con_destiny)

    
    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_consegui_contato_, zendeskusers.email, zendesktickets.created_at from zendesktickets
    
    join zendeskusers on zendeskusers.id = zendesktickets.requester_id
    
    where (custom_fields_consegui_contato_ = 'sim__consegui_contato_com_os_números'
    or custom_fields_conseguiu_falar_com_o_cliente_ = 'sim__consegui_contato_com_os_números'
    or custom_fields_conseguiu_falar_com_o_cliente_ = 'novo_contato_agendado') """
    ticket_not_talked = sqlio.read_sql_query(query, con_pipe)
    ticket_not_talked['id_zendesk_ticket'] = ticket_not_talked.id_zendesk_ticket.astype('int')

    removed=opened_tickets.merge(ticket_not_talked, on= 'id_zendesk_ticket')

    data=data[~data.client_id.isin(removed.client_id)]

    return data


def punish_recall(data):
    #### Revisitar ######
    
    query = """select client_id, created_at created_at_past, id_zendesk_ticket from zendesk_integration

    where created_at > '2020/01/09'
    and formulario_id = '360000663712'"""

    opened_tickets = sqlio.read_sql_query(query, con_destiny)

    query = """select zendesktickets.id id_zendesk_ticket, custom_fields_consegui_contato_ from zendesktickets
    where custom_fields_consegui_contato_ != 'sim__consegui_contato_com_os_números'
    and ticket_form_id = '360000663712' """
    
    ticket_not_talked = sqlio.read_sql_query(query, con_pipe) 
    ticket_not_talked['id_zendesk_ticket'] = ticket_not_talked.id_zendesk_ticket.astype('int')

    lost_hope=opened_tickets.merge(ticket_not_talked, on= 'id_zendesk_ticket')

    losing_hope = lost_hope.groupby('client_id', as_index = False).count()[['client_id','created_at_past']]

    data = data.merge(losing_hope, on = 'client_id', how = 'left' )
    data.created_at_past.fillna(0,inplace = True)
    data['cs'] = data['cs'] - data['created_at_past'] * 0.55 * data['cs']
    data.drop('created_at_past', axis = 1, inplace = True)

    return data


def today_tickets(data):
    
    query = """select client_id, email, created_at created_at_past, id_zendesk_ticket from zendesk_integration
    where created_at > date_trunc('days',now())"""

    opened_ticket_today = sqlio.read_sql_query(query, con_destiny)
    
    data=data[~data.email.isin(opened_ticket_today.email)]

    return data


def insert_data(data, group = 'Rentabilizacao', group_id =360008079852,title = 'Compre de seus fornecedores e não pague antecipação!', form = 'Rentabilizacao', form_id = 360000699252  ,n = 0):
    
    data['created_at'] =  pd.Timestamp.now()
    data['atribuído'] =  group
    data['ticket_name'] = title
    data['atribuido_id'] = group_id
    data['texto_html'] =  data['texto']
    data['formulario'] =  form
    data['formulario_id'] =  form_id
    data['status'] = 'to_send'
    data['id_zendesk_ticket'] =  np.nan
    data['zendesk_ticket_created_at'] = np.nan
    data['zendesk_user_id'] = np.nan
    data['can_be_send'] = True
    
    data = data.sort_values('cs', ascending = False)

    data.head(n).to_sql('zendesk_integration', con_send, index=False, if_exists='append')