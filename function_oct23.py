from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient
import pymongo
import requests
import config
import pandas as pd
import json
from bson.objectid import ObjectId

import dns.resolver
dns.resolver.default_resolver=dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers=['8.8.8.8']

client = MongoClient(f'mongodb+srv://{config.mongo_pat}')
db = client['legacy-api-management']
col_soc = db["societies"]
col_it = db["items"]

def get_conso():
    print("########### GET CONSO START ###########")
    result = client['legacy-api-management']['items'].aggregate([
        {
            '$match': {
                'statusHistory.to': 'confirmed',
                'createdAt': {
                    '$gte': datetime(2022, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
                }
            }
        }, {
            '$group': {
                '_id': {
                    'society_id': '$society._id',
                    'type': '$type',
                    'month_year': {
                        '$dateToString': {
                            'format': '%Y-%m',
                            'date': '$createdAt'
                        }
                    },
                    'offline': '$offline'
                },
                'confirmed_entries': {
                    '$sum': 1
                },
                'confirmed_price_sum': {
                    '$sum': '$price.amount'
                },
                'cancelled_entries': {
                    '$sum': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$status', 'cancelled'
                                ]
                            },
                            'then': 1,
                            'else': 0
                        }
                    }
                },
                'cancelled_price_sum': {
                    '$sum': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$status', 'cancelled'
                                ]
                            },
                            'then': '$cancellation.price.amount',
                            'else': 0
                        }
                    }
                },
                'last_booking_date_by_type': {
                    '$max': '$createdAt'
                },
                'unique_travelers': {
                    '$addToSet': '$travelers.email'
                }
            }
        }, {
            '$project': {
                '_id': 1,
                'confirmed_entries': 1,
                'confirmed_price_sum': 1,
                'cancelled_entries': 1,
                'cancelled_price_sum': 1,
                'last_booking_date_by_type': 1,
                'unique_travelers': {
                    '$size': '$unique_travelers'
                }
            }
        }
    ])

    df = pd.DataFrame(result)
    df = pd.concat([df, pd.json_normalize(df["_id"])], axis=1)

    # Supprimez la colonne "_id" d'origine si nécessaire
    df = df.drop("_id", axis=1)
    df = df.sort_values(by="month_year").reset_index()
    df = df[["society_id","month_year","type","offline",'confirmed_entries','confirmed_price_sum','cancelled_entries','cancelled_price_sum','unique_travelers','last_booking_date_by_type']]
    df.to_csv("results/conso.csv")

def get_society_data():
    print("########### GET SOCIETY DATA START ###########")

    df = pd.read_csv('results/conso.csv')
    df = df.groupby(['society_id',"type","offline","month_year",'last_booking_date_by_type']).sum(numeric_only = True)
    df = df.sort_values(by=["society_id",'type',"month_year"])

    df_group = df.groupby(['society_id']).sum(['confirmed_entries','confirmed_price_sum','cancelled_entries','cancelled_price_sum'])
    df_group = df_group.reset_index()
    df_group = df_group[
        ["society_id",'confirmed_entries', 'confirmed_price_sum', 'cancelled_entries','cancelled_price_sum']]
    # df_group.to_csv("results/extract_group.csv")

    l_name, l_salesName, l_sub_price, l_createdAt = [],[],[],[]
    for i in range (len(df_group)):
        id = (df_group['society_id'][i])
        cursor_soc = col_soc.find({"_id" : ObjectId(id)})
        print(id)
        for c in cursor_soc:
            name=(c['name'])
            try:
                salesName=(c['salesName'])
            except :
                salesName = ""
            sub_price=(c['sub_price'])
            createdAt =(c['createdAt'])
            l_name.append(name)
            l_salesName.append(salesName)
            l_sub_price.append(sub_price)
            l_createdAt.append(createdAt)
    df_group['name']=l_name
    df_group['createdAt']=l_createdAt
    df_group['sub_price']=l_sub_price
    df_group['salesName']=l_salesName
    df_group['total'] = df_group['confirmed_price_sum']-df_group['cancelled_price_sum']
    # df_group.to_csv("results/extract_group.csv")

    l_last = []
    for i in range (len(df_group)):
        id = (df_group['society_id'][i])
        result  = col_it.find_one(
            {"society._id": ObjectId(id)},
            sort=[("_id", pymongo.DESCENDING)])
        # print(result['createdAt'],id)
        l_last.append(result['createdAt'])
    df_group['last_resa'] = l_last
    df_group.to_csv("results/extract_group.csv")

def get_portefeuille():
    print("########### GET PORTEFEUILLE START ###########")

    import requests

    FILTER_ID = 1289

    url = f"https://api.pipedrive.com/v1/organizations?filter_id={FILTER_ID}&limit=500&api_token={config.api_pipedrive}"

    payload = {}
    headers = {
        'Accept': 'application/json',
        'Cookie': '__cf_bm=epS6IiqbeFLh_ZfXxyo.l824MgGVUnpX._S9_Ntj1KA-1693828120-0-AQzPhvtpyCNHrbv5xvIoWIigXcIjKapnnyOvpRQvT2AYGIPIxxr1Yj2+pOp9aj77yICnSx0589w/ZDQX4syB9NU='
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    response = (response.json()['data'])
    l_society_id= []
    l_name= []
    l_owner= []
    l_awarde= []
    l_inactif= []
    l_idpipe= []
    l_signa= []
    l_golive= []
    l_diff_signa_golive = []
    inac = [("763", "✅New"), ("755", "✅Non"),("746", "✅Ponctuel"), ("747", "🛑Oui"),("749", "🤔 Accès démo"),("750", "🤔Test en cours"), ("748", "🤔Test terminé"),("751", "1️⃣ Only one shot")]

    for i in response:
        print(i)
        id_pipe = i["id"]
        id_soc = (i['9d0760fac9b60ea2d3f590d3146d758735f2896d'])
        awarde = (i['446585f9020fe3190ca0fa5ef53fc429ef4b4441'])
        inactif = (i['a056613671b057f83980e4fd4bb6003ce511ca3d'])
        signature = (i['af6c7d5ca6bec13a3a2ac0ffe4f05ed98907c412'])
        golive = (i['24582ea974bfcb46c1985c3350d33acab5e54246'])

        if golive is not None and signature is not None:
            golive_date = datetime.strptime(golive, '%Y-%m-%d')
            signature_date = datetime.strptime(signature, '%Y-%m-%d')
            diff_signa_golive = golive_date - signature_date
            diff_signa_golive = diff_signa_golive.days
            print(diff_signa_golive)
        else:
            diff_signa_golive = ""
            print("Error: golive or signature is None")
        for a,b in inac :
            if inactif == a:
                inactif = b

        owner = (i['owner_id']['name'])
        name = (i['name'])
        l_society_id.append(id_soc)
        l_idpipe.append(id_pipe)
        l_name.append(name)
        l_awarde.append(awarde)
        l_owner.append(owner)
        l_inactif.append(inactif)
        l_signa.append(signature)
        l_golive.append(golive)
        l_diff_signa_golive.append(diff_signa_golive)
    df = pd.DataFrame({'society_id': l_society_id, 'name_org': l_name,'id_pipe':l_idpipe, 'awarde': l_awarde,'owner': l_owner,"inactif" : l_inactif,'signature': l_signa,"golive" : l_golive,'diff_golive' : l_diff_signa_golive})
    df.to_csv("results/pipe2.csv")
    df2 = pd.read_csv('results/extract_group.csv')

    merged_df = pd.merge(df, df2, on='society_id')
    merged_df.to_csv("results/pipe.csv")

def clean_conso_with_actif():
    print("########### CLEAN CONSO START ###########")

    df1 = pd.read_csv("results/extract.csv")
    df2 = pd.read_csv("results/pipe.csv")
    l_res = []
    l_name = []
    l_created = []
    for i in range (len(df1)):
        idsoc = df1['society_id'][i]
        try:
            res = df2.loc[df2["society_id"] == idsoc, "inactif"].values[0]
        except:
            res = "not_found"
        try:
            created = df2.loc[df2["society_id"] == idsoc, "createdAt"].values[0]
        except:
            created = "not_found"
        try:
            name = df2.loc[df2["society_id"] == idsoc, "name_org"].values[0]
        except:
            name = "not_found"

        if idsoc == "5a9d53b472c22dc3c978d67e":
            res = "Interne"
            name = "Supertripper"
        l_res.append(res)
        l_name.append(name)
        l_created.append(created)
    df1['inactif'] = l_res
    df1['name'] = l_name
    df1['created'] = l_created

    df1['offline'] = df1['offline'].replace(True, "Offline").replace(False, "Online")

    df1a = df1[df1["inactif"]!="not_found"]
    df1a = df1a.copy()
    df1a["total_billed"] = df1a["confirmed_price_sum"] - df1a["cancelled_price_sum"]
    df1a.to_csv("results/conso_actif.csv")

    df1b = df1[df1["inactif"]=="not_found"].reset_index()
    df1b['month_year'] = pd.to_datetime(df1b['month_year'])
    df1b = df1b.sort_values(by=['society_id', 'month_year'], ascending=[True, False])
    df1b = df1b.drop_duplicates(subset='society_id', keep='first').sort_values(by=["month_year"], ascending=False)
    df1b = df1b.reset_index(drop=True)
    df1b.to_csv("results/conso_actif_not_found.csv")

def update_last_resa_pipe():
    print("########### GET LAST RESA START ###########")

    l_warning = []
    df = pd.read_csv('results/pipe.csv')
    date_du_jour = datetime.today().date()

    for i in range(len(df)):
        id_pipe = int(df['id_pipe'][i])
        autre_date = (df['last_resa'][i])
        print(autre_date)
        format_str = "%Y-%m-%d %H:%M:%S.%f"
        format_str2 = "%Y-%m-%dT%H:%M:%S.%fZ"
        format_str3 = "%Y-%m-%d %H:%M:%S"
        try:
            autre_date = datetime.strptime(autre_date, format_str)
        except:
            try:
                autre_date = datetime.strptime(autre_date, format_str2)
            except: autre_date = datetime.strptime(autre_date, format_str3)
        autre_date = datetime.date(autre_date)
        difference_en_jours = (date_du_jour - autre_date).days
        l_warning.append(difference_en_jours)

        ## MAJ FIELD
        url = f"https://api.pipedrive.com/v1/organizations/{id_pipe}?api_token={config.api_pipedrive}"

        autre_date_str = autre_date.strftime(format_str)[:10]
        date_du_jour_str = date_du_jour.strftime(format_str)[:10]
        print(autre_date_str,date_du_jour_str,difference_en_jours)

        payload = json.dumps({
            "85a8edbe18b18e20a154af934fcadecfa83cc844": difference_en_jours,
            "e90d8b192b7cb46e84bb21ec7d9a9dfb2e4b4b54": date_du_jour_str,
            "c38c542419257bc687d5cf9393622b22b21788db": autre_date_str
        })

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Cookie': '__cf_bm=9.Hut1jttNLxgvIF4ymsM0cPiM7WJINulQ7KFHWbCrk-1691657460-0-AXRya1f4MbN3X3EmKhUl/PhTge6ufUuOW+FM2+w+zmR5MV67fnsTBnNJXLI4Oi7JxTXfVhGDL6zXrJTqDuJtko8='
        }

        response = requests.request("PUT", url, headers=headers, data=payload)

        print(response.text)

    df['warning_conso'] = l_warning
    df.to_csv('results/pipe.csv')

def get_users_actif():
    result = col_it.aggregate([
        {
            '$unwind': '$travelers'
        }, {
            '$group': {
                '_id': {
                    'society_id': '$society._id',
                    'email': '$travelers.email'
                },
                'firstCreatedAt': {
                    '$first': '$createdAt'
                }
            }
        }, {
            '$group': {
                '_id': {
                    'society_id': '$_id.society_id',
                    'month_year': {
                        '$dateToString': {
                            'format': '%Y-%m',
                            'date': '$firstCreatedAt'
                        }
                    }
                },
                'uniqueTravelersCount': {
                    '$sum': 1
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'society_id': '$_id.society_id',
                'month_year': '$_id.month_year',
                'uniqueTravelersCount': 1
            }
        }
    ])
    df = pd.DataFrame(result)
    df.to_csv("results/users_actif.csv")

def update_sheet():
    print("########### GET UPDATE SHEET START ###########")

    df_conso = pd.read_csv("results/conso_actif.csv")
    df_pipe = pd.read_csv("results/pipe.csv")
    df_conso_out = pd.read_csv("results/conso_actif_not_found.csv")
    df_actif = pd.read_csv("results/users_actif.csv")

    # df_conso['total_billed'] = pd.to_numeric(df_conso['total_billed'], errors='coerce').astype(float)
    df_conso['total_billed'] = df_conso['total_billed'].replace('.',',')
    df_conso['month_year'] = pd.to_datetime(df_conso['month_year'], format='%Y-%m')

    # df_conso['total_billed'] = pd.to_numeric(df_conso['total_billed'], errors='coerce').astype(float)

    df_conso.to_csv("results/conso_actif.csv")
    df_conso = pd.read_csv("results/conso_actif.csv")


    from oauth2client.service_account import ServiceAccountCredentials
    from gspread_pandas import Spread
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file",'https://spreadsheets.google.com/feeds',"https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds/creds_sheet.json", scope)

    # client = gspread.authorize(creds)

    s = Spread('Conso_since_2021')

    s.df_to_sheet(df_conso, sheet='conso_actif', start='A1',replace=True)
    s.df_to_sheet(df_pipe, sheet='pipe', start='A1',replace=True)
    s.df_to_sheet(df_conso_out, sheet='actif_notFound', start='A1',replace=True)
    s.df_to_sheet(df_actif, sheet='users_actif', start='A1',replace=True)