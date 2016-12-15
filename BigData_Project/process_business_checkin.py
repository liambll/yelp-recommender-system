# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re

def find_timezone(x):
   if x=='NV':
       return 0
   elif x=='AZ':
       return 1
   elif (x=='WI') or (x=='IL'):
       return 2
   elif (x=='PA') or (x=='NC') or (x=='SC') or (x=='ON') or (x=='QC'):
       return 3
   elif (x=='EDH' or x=='MLN'):
       return 8
   elif (x=='BW'):
       return 9
   else:
       0
    
input = 'D:\\OneDrive\\Data\\School\\Canada\\Course\\VIVA 101\\Project'
business_data = pd.read_csv(input+'\\yelp_academic_dataset_business.csv')
checkin_data = pd.read_csv(input+'\\yelp_academic_dataset_checkin.csv')

#Business Data
business_data.columns.values
business_data["full_address"] = business_data["full_address"] \
    .apply(lambda x: x.replace('\n',' ').replace('\r',' '))
state = business_data
state = state.groupby(['state']).aggregate('count')
state = state.reset_index()
state = state[state['business_id']>100][['state']]
business_data2 = business_data.merge(state,left_on='state',right_on='state')
business_data2['timezone'] = business_data2['state'].apply(find_timezone)

#Checkin Data
checkin_data = checkin_data.fillna(0)
checkin_data.columns.values
checkin_data2 = checkin_data.select(lambda x: not re.search('type', x), axis=1)

#KayYbHCt-RkbGcPdGOThNg
business_checkin = business_data2.merge(checkin_data2,left_on='business_id',right_on='business_id',how='inner')
business_checkin.fillna(0)
#business_checkin2 = pd.DataFrame(columns=('business_id','day','hour','checkin'))
business_checkin2  = [('business_id','day','hour','checkin')]
        
for record in range(len(business_checkin)):
    business_id = business_checkin.iloc[record]['business_id']
    timezone = business_checkin.iloc[record]['timezone']
    for i in range(7):
        for j in range(0,24):
            i2 = int((i+(j+timezone)//24)%7)
            j2 = int((j+timezone)%24)
            checkin = business_checkin.iloc[record]['checkin_info.'+str(j)+'-'+str(i)]
            business_checkin2 \
                .append((business_id, i2, j2, checkin))

#business_checkin2 = business_checkin2.fillna(0)
#business_checkin[business_checkin['business_id']=='KayYbHCt-RkbGcPdGOThNg']
import csv
with open('yelp_business_checkin.csv','wb') as out:
    csv_out=csv.writer(out)
    for row in business_checkin2:
        csv_out.writerow(row)

#business_checkin2.to_csv('yelp_business_checkin.csv', index=False)
