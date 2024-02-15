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
    l_raison_risk,l_niveau_risk,l_note_risk= [],[],[]
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
        raison_risk = (i['823e303777c6514a9928c900e69aceb32ba44ac2'])
        niveau_risk = (i['6766d4a88e95d61ffd539c683902fa3685e32c92'])
        note_risk = (i['5d68f3fbdc5431990f13f6179c4e9779a5b8de86'])


        if golive is not None and signature is not None:
            golive_date = datetime.strptime(golive, '%Y-%m-%d')
            signature_date = datetime.strptime(signature, '%Y-%m-%d')
            diff_signa_golive = golive_date - signature_date
            diff_signa_golive = diff_signa_golive.days
        else:
            diff_signa_golive = ""
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
        l_niveau_risk.append(niveau_risk)
        l_raison_risk.append(raison_risk)
        l_note_risk.append(note_risk)
        l_diff_signa_golive.append(diff_signa_golive)
    df = pd.DataFrame({'society_id': l_society_id, 'name_org': l_name,'id_pipe':l_idpipe, 'awarde': l_awarde,'owner': l_owner,"inactif" : l_inactif,'signature': l_signa,"golive" : l_golive,'diff_golive' : l_diff_signa_golive,'niveau_risk':l_niveau_risk,'raison_risk':l_raison_risk,'note_risk':l_note_risk})

    mapping = {
        '787': "Conso à l'arrêt + NRP",
        '788': "Produit (techno, fonctionnalités)",
        '791': "Tarif abo",
        '789': "Facturation/Compta",
        '792': "Mauvaise expérience",
        '793': "Consolidation (appartient à un autre groupe ou rachat)",
        '796': "En redressement / Cessation",
        '797': "Réduction des coûts & déplacements",
        '816': "Différence tarifaire sur plateforme"
    }

    df['raison_risk'] = df['raison_risk'].apply(
        lambda x: ', '.join([mapping.get(val, val) for val in str(x).split(',') if val in mapping]))

    df['niveau_risk'] = df['niveau_risk'].replace("794","Moyen").replace("795","Élevé")

    df.to_csv("results/pipe_all.csv")

    df2 = pd.read_csv('results/extract_group.csv')
    merged_df = pd.merge(df, df2, on='society_id')
    merged_df.to_csv("results/pipe.csv")

def get_churn():
    print("########### GET CHURN START ###########")

    import requests

    FILTER_ID = 1450

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
    l_idpipe= []
    l_signa= []
    l_diff_stayed= []
    l_churn= []

    for i in response:
        print(i)
        id_pipe = i["id"]
        id_soc = (i['9d0760fac9b60ea2d3f590d3146d758735f2896d'])
        awarde = (i['446585f9020fe3190ca0fa5ef53fc429ef4b4441'])
        signature = (i['af6c7d5ca6bec13a3a2ac0ffe4f05ed98907c412'])
        churn = (i['eda2124e4e8bed55f7f2642cf3b5238d4bfccd58'])
        owner = (i['owner_id']['name'])
        name = (i['name'])

        if churn is not None and signature is not None:
            golive_date = datetime.strptime(churn, '%Y-%m-%d')
            signature_date = datetime.strptime(signature, '%Y-%m-%d')
            diff_signa_golive = golive_date - signature_date
            diff_signa_golive = diff_signa_golive.days
        else:
            diff_signa_golive = ""
        l_diff_stayed.append(diff_signa_golive)
        l_society_id.append(id_soc)
        l_idpipe.append(id_pipe)
        l_name.append(name)
        l_awarde.append(awarde)
        l_owner.append(owner)
        l_signa.append(signature)
        l_churn.append(churn)
    df = pd.DataFrame({'society_id': l_society_id, 'name_org': l_name,'id_pipe':l_idpipe, 'awarde': l_awarde,'owner': l_owner,'signature': l_signa,"churn" : l_churn,"stayed" : l_diff_stayed,})
    df.to_csv("results/churn.csv")

def clean_conso_with_actif():
    print("########### CLEAN CONSO START ###########")

    df1 = pd.read_csv("results/conso.csv")
    df2 = pd.read_csv("results/pipe.csv")
    l_res = []
    l_name = []
    l_created = []
    l_awarde = []
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
        try:
            awarde = df2.loc[df2["society_id"] == idsoc, "awarde"].values[0]
        except:
            awarde = "not_found"

        if idsoc == "5a9d53b472c22dc3c978d67e":
            res = "Interne"
            name = "Supertripper"
        l_res.append(res)
        l_name.append(name)
        l_created.append(created)
        l_awarde.append(awarde)
    df1['inactif'] = l_res
    df1['name'] = l_name
    df1['created'] = l_created
    df1['awarde'] = l_awarde

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

def dispatch_year():
    df_conso = pd.read_csv("results/conso_actif.csv")
    df_conso['month_year'] = pd.to_datetime(df_conso['month_year'])
    df_conso["billed_22-23"] = df_conso.loc[(df_conso['month_year'] >= '2022-10-01') & (df_conso['month_year'] <= '2023-09-30'), 'total_billed']
    mask = (df_conso['month_year'] >= '2022-10-01') & (df_conso['month_year'] < '2024-10-01')
    df_conso['month_22-24'] = df_conso.loc[mask, 'month_year'].dt.month
    df_conso['year_22-24'] = df_conso.loc[mask, 'month_year'].dt.year
    df_conso["billed_23-24"] = df_conso.loc[(df_conso['month_year'] >= '2023-10-01') & (df_conso['month_year'] <= '2024-09-30'), 'total_billed']

    df_conso.to_csv("results/conso_actif.csv")

def warning():
    import numpy as np

    df = pd.read_csv('results/conso_actif.csv')
    df['month_22-24'] = df['month_22-24'].replace(10,'01*Oct').replace(11,'02*Nov').replace(12,'03*Dec').replace(1,'04*Janv').replace(2,'05*Fevr').replace(3,'06*Mars').replace(4,'07*Avri').replace(5,'08*Mai').replace(6,'09*Juin').replace(7,'10*Juil').replace(8,'11*Aout').replace(9,'12*Sept')
    df = (df.groupby(["society_id",'name',"awarde",'month_22-24']).sum(numeric_only = True)).reset_index()
    df['billed_22-23'] = df['billed_22-23'].round(0)
    df['billed_23-24'] = df['billed_23-24'].round(0)
    df['variation'] = (((df['billed_23-24'] - df['billed_22-23']) / df['billed_22-23']) * 100).round(0)


    df = df[["society_id","name","awarde", "month_22-24", "total_billed", "billed_22-23", "billed_23-24", 'variation']]
    df = df[df['name'] != 'Supertripper'].reset_index()
    df['month_22-24'] = df['month_22-24'].astype(str)
    df['variation'] = df['variation'].astype(float)
    df = df.sort_values(by=['name', 'month_22-24'], ascending=[True, True])

#
    def condition_check(value):
        if value == "inf":
            return 1
        elif pd.to_numeric(value, errors='coerce') > -10:
            return False
        elif pd.to_numeric(value, errors='coerce') <= -10:
            return True
        else:
            return True

    # Appliquer la fonction à la colonne
    df['check_variation'] = df['variation'].apply(condition_check)

    #
    df['awarde mensuel attendu'] = 0
    df['awarde'] = df['awarde'].astype(float)
    # Définir les conditions et appliquer les calculs
    df.loc[df['month_22-24'].str.startswith(('11*', '03*', '10*')), 'awarde mensuel attendu'] = df['awarde'] * 0.05
    df.loc[df['month_22-24'].str.startswith(('02*', '08*')), 'awarde mensuel attendu'] = df['awarde'] * 0.08
    df.loc[df['month_22-24'].str.startswith(('01*', '09*', '04*', '07*')), 'awarde mensuel attendu'] = df['awarde'] * 0.09
    df.loc[df['month_22-24'].str.startswith(('06*', '05*')), 'awarde mensuel attendu'] = df['awarde'] * 0.1
    df.loc[df['month_22-24'].str.startswith('12*'), 'awarde mensuel attendu'] = df['awarde'] * 0.13

    # df['depense vs awarde mensuel attendu'] = (df['billed_23-24']/df['awarde mensuel attendu']*100).round(0)
    # df['depense vs awarde mensuel attendu'] = (df['billed_23-24']/df['awarde mensuel attendu']*100).round(0)
    df['depense vs awarde mensuel attendu'] = (((df['billed_23-24'] - df['awarde mensuel attendu']) / df['awarde mensuel attendu']) * 100).round(0)
    df['check_awarde'] = df['depense vs awarde mensuel attendu'].apply(condition_check)
    df['warning'] = df['check_awarde'] & df['check_variation']
    df.to_csv('results/yeartodate.csv')

def pipe_warning(mois_ecoule):
    df = pd.read_csv('results/yeartodate.csv')
    df_pipe = pd.read_csv('results/pipe_all.csv')
    df = df[df['month_22-24'] == mois_ecoule]
    df_merged = pd.merge(df, df_pipe, on='society_id', how='inner').reset_index()

    l_note, l_objet, l_type, l_account, l_orga = [],[],[],[],[]
    for i in range(len(df_merged)):
        check_variation = (df_merged['check_variation'][i])
        check_awarde = (df_merged['check_awarde'][i])
        warning = (df_merged['warning'][i])
        billed23_24 = df_merged['billed_23-24'][i]
        billed22_23 = df_merged['billed_22-23'][i]
        variation = df_merged['variation'][i]
        variationAw = df_merged['depense vs awarde mensuel attendu'][i]
        waitedAw = df_merged['awarde mensuel attendu'][i]
        name = df_merged['name'][i]
        if warning == True:
            l_objet.append("📉 WARNING AWARDE MENSUEL & CONSO N-1")
            l_note.append(f"[💶 Conso {mois_ecoule} : {billed23_24}€] 🔻 Conso N-1 : {billed22_23}€ soit {variation}% 🔻 AW mensuel estimé : {waitedAw}€ soit {variationAw}%")
        elif warning == False:

            if check_awarde == True:
                l_objet.append("📉 WARNING AWARDE MENSUEL")
                l_note.append(f"[💶 Conso pour {mois_ecoule} : {billed23_24}€] 🔻 AW mensuel estimé : {waitedAw}€ soit {variationAw}%")
            elif check_variation == True:
                l_objet.append("📉 WARNING CONSO N-1")
                l_note.append(f"[💶 Conso {mois_ecoule} : {billed23_24}€] 🔻 Conso N-1 : {billed22_23}€ soit {variation}%")
            else :
                l_objet.append("")
                l_note.append("")
    df_merged["note"] = l_note
    df_merged["objet"] = l_objet
    df_merged["type_activity"] = "🌞 Warning"
    df_merged.drop(columns="level_0",inplace=True)
    df_merged.to_csv('warning.csv')
    df1 = df_merged[(df_merged['inactif'] != "✅Ponctuel") & (df_merged['niveau_risk'].isnull()) & (df_merged['objet'] != "")].reset_index()
    df1["niveau_risk"] = "🔔 A évaluer"
    df1.to_csv('warning_new.csv')
    df2 = df_merged[(df_merged['inactif'] != "✅Ponctuel") & (df_merged['niveau_risk'].notnull())].reset_index(drop=True)
    df2.to_csv('warning_still.csv')

###
    # l_todo_when,l_todo_what,l_done_when,l_done_what,l_email_sent_when,l_email_sent_what,l_email_sent_from,l_email_receive_when,l_email_receive_what,l_email_receive_from = [],[],[],[],[],[],[],[],[],[]
    # for m in range(len(df_pipe)):
    #     idpipe = df_pipe['id_pipe'][m]
    #     url = f"https://api.pipedrive.com/v1/organizations/{idpipe}/flow?api_token={config.api_pipedrive}"
    #
    #     payload = {}
    #     headers = {
    #         'Cookie': '__cf_bm=Bdbo0nwOlddztgS96rmVQbsz3P0mHEsdFLgbR3v3L0E-1707472337-1-AbErhfFlUmzoZ2eYYdIYa46IRSlxXs+/af985mlbp0e9iKehuoNoSAzj+hP0pAy+/3G61zTs3QgAtasDTwRoJ1w='
    #     }
    #
    #     response = requests.request("GET", url, headers=headers, data=payload)
    #     response = (response.json()['data'])
    #     for o in range(len(response)):
    #         object = (response[o]["object"])
    #         if object == "activity":
    #             done = (response[o]['data']['done'])
    #             user_id = (response[o]['data']['user_id'])
    #             if (user_id == 14766484 or user_id == 15232994 or user_id == 15033729) and done is False:
    #                 todo_when = (response[o]['data']['due_date'])
    #                 todo_what= (response[o]['data']['subject'])
    #                 break
    #             else:
    #                 todo_when = ""
    #                 todo_what = ""
    #     for o in range(len(response)):
    #         object = (response[o]["object"])
    #         if object == "activity":
    #             user_id = (response[o]['data']['user_id'])
    #             done = (response[o]['data']['done'])
    #             if (user_id == 14766484 or user_id == 15232994 or user_id == 15033729) and done is True:
    #                 done_when = (response[o]['data']['due_date'])
    #                 done_what = (response[o]['data']['subject'])
    #                 break
    #             else:
    #                 done_when = ""
    #                 done_what = ""
    #     for o in range(len(response)):
    #         object = (response[o]["object"])
    #         if object == "mailMessage" and "supertripper" in response[o]['data']['from'][0]['email_address']:
    #             email_sent_when = (response[o]['data']['timestamp'][:10])
    #             email_sent_what = (response[o]['data']['subject'])
    #             email_sent_from = (response[o]['data']['from'][0]['email_address'])
    #             break
    #         else:
    #             email_sent_when = ""
    #             email_sent_what = ""
    #             email_sent_from =""
    #
    #     for o in range(len(response)):
    #         object = (response[o]["object"])
    #         if object == "mailMessage" and "supertripper" not in response[o]['data']['from'][0]['email_address']:
    #             email_receive_when = (response[o]['data']['timestamp'][:10])
    #             email_receive_what = (response[o]['data']['subject'])
    #             email_receive_from = (response[o]['data']['from'][0]['email_address'])
    #             break
    #         else:
    #             email_receive_when = ""
    #             email_receive_what = ""
    #             email_receive_from = ""
    #     l_todo_when.append(todo_when)
    #     l_todo_what.append(todo_what)
    #     l_done_when.append(done_when)
    #     l_done_what.append(done_what)
    #     l_email_sent_when.append(email_sent_when)
    #     l_email_sent_what.append(email_sent_what)
    #     l_email_sent_from.append(email_sent_from)
    #     l_email_receive_when.append(email_receive_when)
    #     l_email_receive_what.append(email_receive_what)
    #     l_email_receive_from.append(email_receive_from)
    # df_pipe['todo_when'] = l_todo_when
    # df_pipe['todo_what'] = l_todo_what
    # df_pipe['done_when'] = l_done_when
    # df_pipe['done_what'] = l_done_what
    # df_pipe['email_sent_when'] = l_email_sent_when
    # df_pipe['email_sent_what'] = l_email_sent_what
    # df_pipe['email_sent_from'] = l_email_sent_from
    # df_pipe['email_receive_when'] = l_email_receive_when
    # df_pipe['email_receive_what'] = l_email_receive_what
    # df_pipe['email_receive_from'] = l_email_receive_from
    # df_pipe.to_csv('pipe_all.csv')
def update_sheet():
    print("########### GET UPDATE SHEET START ###########")

    df_conso = pd.read_csv("results/conso_actif.csv")
    df_pipe = pd.read_csv("results/pipe.csv")
    df_pipe_all = pd.read_csv("results/pipe_all.csv")
    df_conso_out = pd.read_csv("results/conso_actif_not_found.csv")
    df_actif = pd.read_csv("results/users_actif.csv")
    df_churn = pd.read_csv("results/churn.csv")
    df_ytd = pd.read_csv("results/yeartodate.csv")

    # df_conso['total_billed'] = pd.to_numeric(df_conso['total_billed'], errors='coerce').astype(float)
    df_conso['total_billed'] = df_conso['total_billed'].replace('.',',')
    # df_conso['total_billed'] = df_conso['total_billed'].ast
    df_conso['month_year'] = pd.to_datetime(df_conso['month_year'], format='%Y-%m-%d')

    df_conso['total_billed'] = pd.to_numeric(df_conso['total_billed'], errors='coerce').astype(float)

    df_conso.to_csv("results/conso_actif.csv")
    df_conso = pd.read_csv("results/conso_actif.csv")


    from oauth2client.service_account import ServiceAccountCredentials
    from gspread_pandas import Spread
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file",'https://spreadsheets.google.com/feeds',"https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds/creds_sheet.json", scope)

    # client = gspread.authorize(creds)

    s = Spread('Conso_since_2021')

    s.df_to_sheet(df_conso, sheet='conso_actif', start='A1',replace=True)
    s.df_to_sheet(df_pipe, sheet='pipe_actif', start='A1',replace=True)
    s.df_to_sheet(df_pipe_all, sheet='pipe_all', start='A1',replace=True)
    s.df_to_sheet(df_conso_out, sheet='actif_notFound', start='A1',replace=True)
    s.df_to_sheet(df_actif, sheet='users_actif', start='A1',replace=True)
    s.df_to_sheet(df_churn, sheet='churn', start='A1',replace=True)
    s.df_to_sheet(df_ytd, sheet='yeartodate', start='A1',replace=True)