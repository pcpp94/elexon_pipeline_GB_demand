import sys
import pandas as pd
import requests
import datetime
import xmltodict as xml
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType

from ..utils_others.utils import bmu_types, neta_name_dic, ngc_types, ngc_name_dic, power_stations_dic, standarize_dic, fields_enapp_dic

# If you pay and have acces to the API follow the below.
# Otherwise refer to the README.md

username = 'myuser'
mypassword = 'mypass'

def load_dictionary_enappsys():

  ######## EnAppSys API - BMUs Classifications - XML files

  save_path = "/tmp/delta/EnAppSys_Dictionary_API"

  schema = StructType([StructField('bmu_id',StringType(),True),StructField('player',StringType(),True),StructField('bsc_party',StringType(),True),StructField('ngc_bmu_id',StringType(),True),StructField('ngc_bmu_flag',BooleanType(),True),StructField('last_flag',BooleanType(),True),StructField('currently_active',BooleanType(),True),StructField('effective_from_date',TimestampType(),True),StructField('effective_to_date',StringType(),True),StructField('generation_capacity',DoubleType(),True),StructField('demand_capacity',DoubleType(),True),StructField('enapp_name',StringType(),True),StructField('enapp_class',StringType(),True),StructField('neta_type',StringType(),True),StructField('neta_name',StringType(),True),StructField('ngc_type',StringType(),True),StructField('ngc_name',StringType(),True),StructField('power_station_id',StringType(),True),StructField('power_station_name',StringType(),True),StructField('all_notify',BooleanType(),True),StructField('notify_not_sva',BooleanType(),True),StructField('notify_cva',BooleanType(),True),StructField('notify_sva',BooleanType(),True),StructField('embedded_cva',BooleanType(),True),StructField('directcon_cva',BooleanType(),True),StructField('intercons',BooleanType(),True),StructField('sva_only',BooleanType(),True)])
  

  # EnAppSys Fuel/Category Dictinary
  date_format = datetime.date.today() - datetime.timedelta(45)
  date = str(datetime.date.today() - datetime.timedelta(45)).replace('-','')
  df_final = pd.DataFrame()

  for en_app_bmu in bmu_types:
    r = requests.get(f'https://www.netareports.com/dataService?rt=bmusumddl&username={username}&password={mypassword}=BMUSUM&retType=ENAPPSYSFT&agg='+en_app_bmu+'&startSD='+date+'&endSD='+date+'&sdt1=GC&sdt2=DC')
    r_dict = xml.parse(r.text)
    # Get the XML into a DataFrame format
    try: 
      data = pd.DataFrame.from_dict(r_dict['response']['settlementdate']['bmunit'])
      data.columns = data.columns.str.replace('@','')
      for i in range(len(data)):
        if i == 0:
          settlement_date = r_dict['response']['settlementdate']['@settlementdate']
          neta_type = en_app_bmu
          df = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df.columns = df.columns.str.replace('@','')
          df['settlement_date'] = settlement_date
          df['neta_type'] = neta_type
          df['bmu_id'] = data['bmunitid'][i]
        else:
          df_aux = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df_aux.columns = df_aux.columns.str.replace('@','')
          df_aux['settlement_date'] = settlement_date
          df_aux['neta_type'] = neta_type
          df_aux['bmu_id'] = data['bmunitid'][i] 
          df = pd.concat([df,df_aux])
      df['settlementperiod'] = pd.to_numeric(df['settlementperiod'])
      df['gc'] = pd.to_numeric(df['gc'])
      df['dc'] = pd.to_numeric(df['dc'])
      df = df.rename(columns={'gc': 'generation_capacity', 'dc':'demand_capacity'})
      df = df.drop(columns=['settlementperiod']).groupby(by=['bmu_id','settlement_date','neta_type']).mean().reset_index()
      df_final = pd.concat([df_final,df])
    except: pass

  df_final['neta_name'] = df_final['neta_type'].map(neta_name_dic)


  # National Grid ID Classification by BMU Id
  date = str(datetime.date.today() - datetime.timedelta(45)).replace('-','')
  df_final_2 = pd.DataFrame()

  for ngc_type in ngc_types:
    r_2 = requests.get('https://www.netareports.com/dataService?rt=bmusumddl&username={username}&password={mypassword}=BMUSUM&retType=NGCFT&agg='+ngc_type+'&startSD='+date+'&endSD='+date+'&sdt1=GC&sdt2=DC')
    r_dict_2 = xml.parse(r_2.text)
    # Get the XML into a DataFrame format
    try: 
      data = pd.DataFrame.from_dict(r_dict_2['response']['settlementdate']['bmunit'])
      data.columns = data.columns.str.replace('@','')
      for i in range(len(data)):
        if i == 0:
          settlement_date = r_dict_2['response']['settlementdate']['@settlementdate']
          ngc_type = ngc_type
          df = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df.columns = df.columns.str.replace('@','')
          df['settlement_date'] = settlement_date
          df['ngc_type'] = ngc_type
          df['bmu_id'] = data['bmunitid'][i]
        else: 
          df_aux = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df_aux.columns = df_aux.columns.str.replace('@','')
          df_aux['settlement_date'] = settlement_date
          df_aux['ngc_type'] = ngc_type
          df_aux['bmu_id'] = data['bmunitid'][i] 
          df = pd.concat([df,df_aux])
      df['settlementperiod'] = pd.to_numeric(df['settlementperiod'])
      df['gc'] = pd.to_numeric(df['gc'])
      df['dc'] = pd.to_numeric(df['dc'])
      df = df.rename(columns={'gc': 'generation_capacity', 'dc':'demand_capacity'})
      df = df.drop(columns=['settlementperiod']).groupby(by=['bmu_id','settlement_date','ngc_type']).mean().reset_index()
      df_final_2 = pd.concat([df_final_2,df])
    except: pass

  df_final_2 = df_final_2[df_final_2['ngc_type'] != 'NA']
  df_final_2['ngc_name'] = df_final_2['ngc_type'].map(ngc_name_dic)


  # Power Station Names by BMU Id
  ps_list = list(power_stations_dic.keys())
  date = str(datetime.date.today() - datetime.timedelta(45)).replace('-','')
  df_final_3 = pd.DataFrame()

  for power_station_id in ps_list:
    r_3 = requests.get(f'https://www.netareports.com/dataService?rt=bmusumddl&username={username}&password={mypassword}=BMUSUM&retType=PS&agg='+power_station_id+'&startSD='+date+'&endSD='+date+'&sdt1=GC&sdt2=DC')
    r_dict_3 = xml.parse(r_3.text)
    # Get the XML into a DataFrame format
    try: 
      data = pd.DataFrame.from_dict(r_dict_3['response']['settlementdate']['bmunit'])
      data.columns = data.columns.str.replace('@','')
      for i in range(len(data)):
        if i == 0:
          settlement_date = r_dict_3['response']['settlementdate']['@settlementdate']
          power_station_id = power_station_id
          df = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df.columns = df.columns.str.replace('@','')
          df['settlement_date'] = settlement_date
          df['power_station_id'] = power_station_id
          df['bmu_id'] = data['bmunitid'][i]
        else: 
          df_aux = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df_aux.columns = df_aux.columns.str.replace('@','')
          df_aux['settlement_date'] = settlement_date
          df_aux['power_station_id'] = power_station_id
          df_aux['bmu_id'] = data['bmunitid'][i] 
          df = pd.concat([df,df_aux])
      df['settlementperiod'] = pd.to_numeric(df['settlementperiod'])
      df['gc'] = pd.to_numeric(df['gc'])
      df['dc'] = pd.to_numeric(df['dc'])
      df = df.rename(columns={'gc': 'generation_capacity', 'dc':'demand_capacity'})
      df = df.drop(columns=['settlementperiod']).groupby(by=['bmu_id','settlement_date','power_station_id']).mean().reset_index()
      df_final_3 = pd.concat([df_final_3,df])
    except: pass

  df_final_3['power_station_name'] = df_final_3['power_station_id'].map(power_stations_dic)


  # BMUs by high level Flag --> Notify, is SVA, is CVA, etc.
  date = str(datetime.date.today() - datetime.timedelta(45)).replace('-','')
  bmu_class = ['ALLNOTIFY',	'NOTIFYNOTSVA',	'NOTIFYCVA',	'NOTIFYSVA',	'EMBEDDEDCVA',	'DIRECTCONCVA',	'INTERCONS',	'SVAONLY']
  df_final_4 = pd.DataFrame()

  for bmu_type in bmu_class:
    r_4 = requests.get(f'https://www.netareports.com/dataService?rt=bmusumddl&username={username}&password={mypassword}=BMUSUM&retType=BMUTYPE&bmutr='+bmu_type+'&startSD='+date+'&endSD='+date+'&sdt1=GC&sdt2=DC')
    r_dict_4 = xml.parse(r_4.text)
    # Get the XML into a DataFrame format
    try: 
      data = pd.DataFrame.from_dict(r_dict_4['response']['settlementdate']['bmunit'])
      data.columns = data.columns.str.replace('@','')
      for i in range(len(data)):
        if i == 0:
          settlement_date = r_dict_4['response']['settlementdate']['@settlementdate']
          col_name = str(bmu_type)
          df = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df.columns = df.columns.str.replace('@','')
          df['settlement_date'] = settlement_date
          df.loc[:,col_name] = True
          df['bmu_id'] = data['bmunitid'][i]
        else: 
          df_aux = pd.DataFrame.from_dict(data['settlementperiod'][i])
          df_aux.columns = df_aux.columns.str.replace('@','')
          df_aux['settlement_date'] = settlement_date
          df_aux.loc[:,col_name] = True
          df_aux['bmu_id'] = data['bmunitid'][i] 
          df = pd.concat([df,df_aux])
      df['settlementperiod'] = pd.to_numeric(df['settlementperiod'])
      df['gc'] = pd.to_numeric(df['gc'])
      df['dc'] = pd.to_numeric(df['dc'])
      df = df.rename(columns={'gc': 'generation_capacity', 'dc':'demand_capacity'})
      df = df.drop(columns=['settlementperiod'])
      df_final_4 = pd.concat([df_final_4,df])
    except: pass

  df_final_4 = df_final_4.reset_index(drop=True)


  # Get the last variables to add: BSC Party, etc.
  r_5 = requests.get(f'https://www.netareports.com/dataService?rt=bmuforbscp&username={username}&password={mypassword}&orderby=bmu')
  df_final_5 = pd.DataFrame.from_dict(xml.parse(r_5.text)['response']['bmunitforbscparty'])
  df_final_5.columns = df_final_5.columns.str.replace('@','')
  df_final_5['effectivetodate'] = df_final_5['effectivetodate'].fillna("True")
  df_final_5['effectivefromdate'] = pd.to_datetime(df_final_5['effectivefromdate'])
  df_final_5 = df_final_5.rename(columns={'bmunit' : 'bmu_id'})

  r_6 = requests.get(f'https://www.netareports.com/dataService?rt=bmunit&username={username}&password={mypassword}')
  r_dict_6 = xml.parse(r_6.text)
  df = pd.DataFrame(r_dict_6['response']['bmunit'])
  df.columns = df.columns.str.replace('@','')
  df = df.rename(columns={'bmunitid' : 'bmu_id'})


  # Merging and cleaning.
  bmu_dictionary = df_final_5.merge(df_final, how='left', left_on = 'bmu_id', right_on='bmu_id')
  bmu_dictionary = bmu_dictionary.merge(df_final_2, how='left', left_on = 'bmu_id', right_on='bmu_id')
  bmu_dictionary = bmu_dictionary.merge(df_final_3, how='left', left_on = 'bmu_id', right_on='bmu_id')

  for bmu_type in bmu_class:
    bmu_dictionary = bmu_dictionary.merge(df_final_4[['bmu_id', 'settlement_date', 'generation_capacity', 'demand_capacity', bmu_type]].groupby(by=['settlement_date', 'bmu_id', bmu_type]).mean().reset_index().dropna(), how='left', left_on = 'bmu_id', right_on='bmu_id')

  bmu_dictionary = bmu_dictionary.reset_index(drop=True)
  bmu_dictionary['generation_capacity'] = bmu_dictionary[['generation_capacity_x', 'generation_capacity_y']].mean(axis=1)
  bmu_dictionary['demand_capacity'] = bmu_dictionary[['demand_capacity_x', 'demand_capacity_y']].mean(axis=1)
  bmu_dictionary['settlement_date'] = date_format
  bmu_dictionary = bmu_dictionary.drop(columns=['generation_capacity_x', 'generation_capacity_y', 'demand_capacity_x', 'demand_capacity_y', 'settlement_date_x', 'settlement_date_y', 'settlement_date'])
  bmu_dictionary = bmu_dictionary.merge(df, how='left', left_on='bmu_id', right_on='bmu_id')
  bmu_dictionary = bmu_dictionary.reset_index(drop=True)
  bmu_dictionary = bmu_dictionary.rename(columns=standarize_dic)
  bmu_dictionary['ngc_bmu_flag'] = bmu_dictionary['ngc_bmu_id'].apply(lambda x: True if type(x) == str else False)
  bmu_dictionary = bmu_dictionary.merge(bmu_dictionary[['bmu_id', 'effective_from_date']].groupby('bmu_id').max().reset_index().rename(columns={'effective_from_date' : 'max_date'}), how='left', left_on='bmu_id', right_on ='bmu_id')
  bmu_dictionary['last_flag'] = bmu_dictionary['effective_from_date'] == bmu_dictionary['max_date']
  bmu_dictionary['currently_active'] = bmu_dictionary['effective_to_date'].apply(lambda x: True if x == 'True' else False)
  bmu_dictionary = bmu_dictionary.drop(columns='max_date')
  bmu_dictionary['player'] = bmu_dictionary['bmu_id'].str.slice(stop=2)
  bmu_dictionary = bmu_dictionary[fields_enapp_dic]

  spark.createDataFrame(bmu_dictionary, schema=schema).write.format('delta').save(save_path)

# DATABRICKS: Saved as elexon_etl.enappsys_bmu_dictionary
