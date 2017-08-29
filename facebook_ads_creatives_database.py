# -*- coding: utf-8 -*-
"""
Created on Mon May  8 13:52:50 2017

@author: Sangwoo
"""

from facebookads.api import FacebookAdsApi
from facebookads import objects
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.ad import Ad
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, MetaData
import numpy as np
import re
from facebookads.adobjects.adcreative import AdCreative
from facebookads.adobjects.adimage import AdImage
from facebookads.adobjects.adsinsights import AdsInsights

account_id = "act_00000000000000000"

my_app_id = '0000000000000'
my_app_secret = '00000000000000000000000000000000'
my_access_token = '000000000000000000000000000000000000000'



FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)



class creatives:
    
    def get_creatives(self, report_date, account_id):
        ad_account = AdAccount(fbid=account_id)
        limit = 500
        fields = [AdCreative.Field.thumbnail_url]
        params = {
        'time_range': {
                'since': report_date,
                'until': report_date
            },
#        'date_preset':'last_90d',
        'limit':limit if limit > 0 else None 
        }
        insights = ad_account.get_ad_creatives(fields, params)
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
            'creative_id':'creatives_id',
            'thumbnail_url':'image_url',
            }
#        key_value = {'start_date':report_date, 'end_date':end_date}
    
        key_value = dict.fromkeys(general_columns.values())
#        key_value.update(d)
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
            insights = creatives.get_creatives(report_date, account_id)
            self.ap.append(insights)
    
    def lists(self):
        self.holds = []
        arrange = filter(None, self.ap)
        for i in arrange:
            for a in i:
                self.holds.append(a)
    
    def frames(self):
        dicts = creatives.get_insights_value(self.holds)        
        df = pd.DataFrame(dicts)
        try:
            header = self.df.iloc[0]
            self.df = self.df[1:]
            self.df = self.df.rename(columns = header).fillna(0)
        except FacebookRequestError:
            pass
        return df
    


#notyet, start from here
start_date = "2017-08-09"
end_date = "2017-08-16"  
creatives = creatives()

data = dataframe(start_date, end_date)

data.dicts()
data.lists()
DF = data.frames()

header = DF.iloc[0]
DF = DF[1:]
DF = DF.rename(columns = header).fillna(0)




































