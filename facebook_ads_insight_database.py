# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 11:57:15 2017

@author: Sangwoo
"""

from facebookads.api import FacebookAdsApi
#from facebookads import objects
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.ad import Ad
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, MetaData
import numpy as np
import re


account_id = "act_00000000000000000"

my_app_id = '0000000000000'
my_app_secret = '00000000000000000000000000000000'
my_access_token = '000000000000000000000000000000000000000'



FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)


class AdsInsight:
    

    def get_ads_insight(self, report_date,account_id):

        ad_account = AdAccount(fbid=account_id)
        limit = 5000
        fields = ['campaign_name',
                'ad_id',
                'reach',
                'date_start',
                'date_stop',
                'impressions',
                'actions',
                'ctr',
                'clicks',
                'objective',
                'spend'
            ]
        params = {
            'time_range': {
                'since': report_date,
                'until': report_date
            },
#            'date_preset':'last_7d',
            'action_attribution_windows': ['28d_click','28d_view'],
            'breakdowns': ['gender', 'age'],
            'time_increment':'1',             
            'level': 'ad',
            'limit': limit if limit > 0 else None
        }
        insights = ad_account.get_insights(fields, params)
        return insights
    
    def get_insights_value(self, insights, limit=-1):

        count = 0
        data = []
        key_value = None
        for insight in insights:
            key_value = self.get_values(insight)
            if count == 0:
                data.append(key_value.keys())
            data.append(key_value.values())
            count += 1
            if limit > 0 and limit == count:
                break
        return data
    
    def get_values(self,insight):

        # action values that we want to get from insight
        # format is action_value_returned_from_api: db_column_name
        action_type_columns = {
            "offsite_conversion.fb_pixel_purchase":'purchase'
        }
        # general columns that we want to get from insight
        general_columns = {
            'gender': 'gender',
            'age':'age',
            'campaign_name': 'campaign_name',
            'impressions':'impression',
            'ad_id': 'ad_id',
            'date_start':'date_start',
            'date_stop':'date_stop',
            'objective':'objective',
            'reach': 'reach',
            'ctr':'ctr',
            'clicks':'clicks',
            'spend':'spend'
        }
#        key_value = {'start_date': report_date, 'end_date': end_date}
        key_value = dict.fromkeys(action_type_columns.values())
        d3 = dict.fromkeys(general_columns.values())
        key_value.update(d3)
        # get values for general columns
        for k in general_columns.keys():
            if k in insight:
                key_value[general_columns[k]] = \
                    str(insight[k]).replace("'", r"\'")
        # get values for action values
        if 'actions' in insight:
            actions = insight['actions']
            for action in actions:
                t = action['action_type']
                if t in action_type_columns and '28d_click' not in action:
                    key_value[action_type_columns[t]] = \
                        str(int(action["28d_view"]))
                elif t in action_type_columns and '28d_view' not in action:
                    key_value[action_type_columns[t]] = \
                        str(int(action["28d_click"]))
                elif t in action_type_columns and '28d_view' in action and '28d_click' in action:
                    key_value[action_type_columns[t]] = \
                        str(int(action["28d_click"]) + int(action["28d_view"]))
        return key_value
    



class dataframe:
    
    def __init__(self, start_date, end_date):
        self.daterange = pd.date_range(start_date, end_date).strftime('%Y-%m-%d')

    
    def dicts(self):
        self.ap = []
        for report_date in self.daterange:
            insights = insight.get_ads_insight(report_date, account_id)
            self.ap.append(insights)
    
    def lists(self):
        self.holds = []
        arrange = filter(None, self.ap)
        for i in arrange:
            for a in i:
                self.holds.append(a)
    
    def frames(self):
        dicts = insight.get_insights_value(self.holds)        
        df = pd.DataFrame(dicts)
        try:
            header = df.iloc[0]
            df = df[1:]
            df = df.rename(columns = header).fillna(0)
        except IndexError:
            pass
        return df



###########################################################
start_date = "2017-01-10"
end_date = "2017-01-16"  #finished collection
insight = AdsInsight()
data = dataframe(start_date, end_date)
data.dicts()
data.lists()
df = data.frames()








    
    