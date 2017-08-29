# -*- coding: utf-8 -*-
"""
Created on Mon May  8 13:52:50 2017

@author: Sangwoo
"""

from facebookads.api import FacebookAdsApi
#from facebookads.adobjects.adcreative import AdCreative
#from facebookads.adobjects.adimage import AdImage
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.ad import Ad

import pandas as pd
#import numpy as np
import re

import psycopg2
from sqlalchemy import create_engine, MetaData
import sqlalchemy

account_id = "act_00000000000000000"

my_app_id = '0000000000000'
my_app_secret = '00000000000000000000000000000000'
my_access_token = '000000000000000000000000000000000000000'



FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)


class Ads:
    
    def ads(self, report_date, account_id):
        ad_account = AdAccount(fbid=account_id)
        limit = 5000
        fields = ['id','creative', Ad.Field.name]
        params = {
        'time_range': {
                'since': report_date,
                'until': report_date
            },
#        'date_preset':'last_90d',
        'limit':limit if limit > 0 else None 
        }
        insights = ad_account.get_ads(fields, params)
        return insights
    
    def get_insights_value(self, insights, limit=-1):
        count = 0
        data = []
        key_value = None
        insights = insights[0:len(insights)]
        for insight in insights:
            key_value = self.get_values(insight)
            if count == 0:
                data.append(key_value.keys())
            data.append(key_value.values())
            count+= 1
            if limit > 0 and limit==count:
                break
        return data
    
    def get_values(self, insight):
        general_columns = {
            'id':'ad_id',
            'creative':'creatives_id',
            'name':'name'
            }
    
        key_value = dict.fromkeys(general_columns.values())
        for k in general_columns.keys():
            if k in insight:
                key_value[general_columns[k]] = str(insight[k])
        return key_value
    

class dataframe:
    
    def __init__(self, start_date, end_date):
        self.daterange = pd.date_range(start_date, end_date).strftime('%Y-%m-%d')

    
    def dicts(self):
        self.ap = []
        for report_date in self.daterange:
            insights = advertise.ads(report_date, account_id)
            self.ap.append(insights)
    
    def lists(self):
        self.holds = []
        arrange = filter(None, self.ap)
        for i in arrange:
            for a in i:
                self.holds.append(a)
    
    def frames(self):
        dicts = advertise.get_insights_value(self.holds)        
        self.df = pd.DataFrame(dicts)
        try:
            header = self.df.iloc[0]
            self.df = self.df[1:]
            self.df = self.df.rename(columns = header).fillna(0)
        except IndexError:
            pass
    
    def perfect(self):
        ids = self.df.creatives_id.values
        cre = []
        for k in range(0, len(ids), 1):
            j = re.findall(r'\d{13}',ids[k])[0]
            cre.append(j)
        per = pd.DataFrame({'ad_id':self.df.ad_id, 'creatives_id':cre})
        return per


class keyword_stats:
    
    def __init__(self, df):
        self.ad_ids = df.ad_id.values

    def key_raw(self):
        self.put = []
        for ad_id in self.ad_ids:
            ad = Ad(ad_id)
            key =  ad.get_keyword_stats(fields=['id', 'name', 'clicks','impressions','reach','actions'])
            self.put.append(key)

    def key_transform(self):
        hold = []
        lists = filter(None, self.put)
        for j in lists:
            keys = j 
            for i in keys[0]:
                key = keys[0][i]
                hold.append(key)
        self.fil = filter(lambda v: v is not None, hold)  
        
    def key_frame(self):
        key_list = []
        for k in self.fil:
            keys = self.keystat(k)
            key_list.append(keys)
        df = pd.DataFrame(key_list).fillna(0)
        return df

    def keystat(self, key):
        action_columns = {
                    'offsite_conversion.fb_pixel_aggregate_custom_conversion': 'purchase'
          }
        
        general_columns = {
                    'reach': 'reach',
                    'id': 'key_id',
                    'impressions':'impression',
                    'name': 'name',
                    'clicks':'clicks'
            }

        key_value = dict.fromkeys(action_columns.values())
        d1 = dict.fromkeys(general_columns.values())
        key_value.update(d1)
    #if -> for
        for k in general_columns.keys():
            if k in key:
               key_value[general_columns[k]] = \
                        str(key[k]).replace("'",r"\'")

        if 'actions' in key:
           actions = key['actions']
           for action in actions:
               t = action['action_type']
               if t in action_columns and 'value' in action:
                   key_value[action_columns[t]] =\
                        str(action['value'])
        return key_value



 
start_date = "2017-07-01"
end_date = "2017-08-08"  
#report_date = "2017-07-19"
#adss = adss(report_date, account_id)

################################ad_id
advertise = Ads()
data = dataframe(start_date, end_date)

data.dicts()
data.lists()
data.frames()
DF = data.perfect()





####################################keyword
keyword = keyword_stats(DF)
keyword.key_raw()
keyword.key_transform()
key = keyword.key_frame()


































