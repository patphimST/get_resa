import time
import json
import pandas as pd
from pymongo import MongoClient
import config
import requests
from datetime import datetime
from bson.objectid import ObjectId

# client = MongoClient(f'mongodb+srv://{config.mongo_pat}')
# db = client['legacy-api-management']
# col_soc = db["societies"]
# col_it = db["items"]
# col_bills = db["bills"]
# col_users = db["users"]

df_code = pd.read_csv(f'csv/AO&D _2023.csv')
df_hotel = pd.read_csv(f'csv/hotel.csv')

def clean_volume_flight_for_all():
    name = "all_flight2023"
    df = pd.read_csv(f'csv/{name}.csv')

    columns_to_concat = ['_id.des[0][0]','_id.des[0][1]','_id.des[0][2]','_id.ori[0][0]','_id.ori[0][1]','_id.ori[0][2]']

    df['OD_mixed'] = df[columns_to_concat].apply(lambda row: ','.join(map(str, row)), axis=1)
    df['OD_mixed'] = df['OD_mixed'].str.replace(',nan',"")
    df['OD_mixed'] = df['OD_mixed'].str.replace('nan,',"")

    def check_fr(value):
        if 'FR,FR' in value or 'FR,FR,FR,FR' in value:
            return 'FR'
        else:
            return 'Autre'

    df['zone'] = df['OD_mixed'].apply(lambda x: check_fr(x))
    df = df[["_id.id","_id.createdAt","_id.status","totalPriceConfirmed","OD_mixed","totalTravelers","zone"]]

    zone_seg = []
    ##segment
    for a in range(len(df)):
        if df["zone"][a] == "FR":
            zoneinfo = "FRANCE"
        elif df["zone"][a] == "Autre":
            try:
                r = df["OD_mixed"][a].replace('FR,',"").replace(',FR',"")
            except:
                r = df["OD_mixed"][a]
            r_l = (r.split(','))
            l_z = []
            for i in r_l:
                print(i)
                try:
                    s = df_code.loc[df_code['Country code of destination'] == i,'Geographical area of destination'].values[0]
                    l_z.append(s)
                except:
                    i = 'FR'
                    s = df_code.loc[df_code['Country code of destination'] == i,'Geographical area of destination'].values[0]
                    l_z.append(s)
            if "EUROPE" not in l_z:
                zoneinfo = "MONDE"
            else:
                zoneinfo = "EUROPE"
        zone_seg.append(zoneinfo)
    df['ZONE_SEGMENT'] = zone_seg
    df.to_csv(f'csv/{name}_clean.csv')

def clean_volume_train_for_all():
    name = "all_train2023"
    df = pd.read_csv(f'csv/{name}.csv')

    columns_to_concat = ["_id.des[0]","_id.des[1]","_id.ori[0]","_id.ori[1]"]

    # Concaténez les valeurs de chaque ligne des colonnes sélectionnées avec une virgule comme délimiteur
    df['OD_mixed'] = df[columns_to_concat].apply(lambda row: ','.join(map(str, row)), axis=1)
    df['OD_mixed'] = df['OD_mixed'].str.replace(',nan',"")
    zone_seg = []
    ##segment
    for a in range(len(df)):
        zone = df['OD_mixed'][a].split(',')
        print(zone)
        l_z =[]
        for i in zone:
            if i.startswith('FR') or i == "nan" :
                z = ('FRANCE')
            else:
                z = ('EUROPE')
            l_z.append(z)
            if "EUROPE" not in l_z:
                zoneinfo = "FRANCE"
            else:
                zoneinfo = "EUROPE"
        zone_seg.append(zoneinfo)
    df['ZONE_SEGMENT'] = zone_seg
    df = df[["_id.id","_id.status","_id.offline","totalTravelers","totalPriceConfirmed","_id.createdAt","OD_mixed","ZONE_SEGMENT"]]
    df['PU'] = df['totalPriceConfirmed']/df['totalTravelers']
    df.to_csv(f'csv/{name}_clean.csv')

def clean_volume_car_for_all():
    df = pd.read_csv('csv/car.csv')
    df = df.fillna("-")
    df["_id.country"] = df["_id.country"].str.upper()
    zone_seg = []
    for a in range(len(df)):
        try :
            count = df['_id.country'][a]
            s = df_code.loc[df_code['Label country of destination'] == count, 'Geographical area of destination'].values[0]
        except:
            try:
                cocode = df['_id.countryCode'][a].upper()
                s = df_code.loc[df_code['Country code of destination'] == cocode, 'Geographical area of destination'].values[0]
            except:
                s = ""
        zone_seg.append(s)
    df['ZONE_SEGMENT'] = zone_seg
    df = df[["_id.id","_id.status","_id.offline","_id.createdAt","totalPriceConfirmed","totalPriceCancelled","_id.country","_id.countryCode","ZONE_SEGMENT"]]
    df.to_csv('csv/car_clean.csv')

def clean_volume_hotel_for_all():
    name = "all_hotel2023"
    df = pd.read_csv(f'csv/{name}.csv',delimiter=";")
    l_ville = []
    # df['annee'] = df['_id.createdAt'].str[:4]
    for i in range(len(df)):
        cocode = df['Zone'][i].strip().upper()
        print(cocode)
        try:
            s = df_code.loc[df_code['Label country of destination'] == cocode, 'Geographical area of destination'].values[0]
            print(s)
        except:
            s = "NF"
        l_ville.append(s)
    df['Zone2'] = l_ville

    df.to_csv(f'csv/{name}_clean.csv')
