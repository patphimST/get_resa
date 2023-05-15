import pandas as pd
from pymongo import MongoClient
import config
from datetime import datetime
from bson.objectid import ObjectId

client = MongoClient(f'mongodb+srv://{config.mongo_pat}')
db = client['legacy-api-management']
col_soc = db["societies"]
col_it = db["items"]
col_bills = db["bills"]


def get_portefeuille():
    print("** Start : Get Data from Portefeuille")

    import gspread
    import csv
    from oauth2client.service_account import ServiceAccountCredentials

    # définir les informations d'identification pour accéder au fichier Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds/creds_sheet.json', scope)
    client = gspread.authorize(creds)

    # ouvrir le fichier et la feuille spécifique que vous souhaitez exporter
    worksheet = client.open('Portefeuille AM').worksheet('DASHBOARD')
    # récupérer toutes les données de la feuille
    data = worksheet.get_all_values()
    # écrire les données dans un fichier CSV
    with open('results/Portefeuille AM.csv', 'w', newline='',encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    print("** End : Get Data from Portefeuille")

def referential_month():
    print("** Start : Create referential")
    import pandas as pd

    # Créer un DataFrame avec toutes les combinaisons de mois et de sociétés.
    df_am = pd.read_csv(f'results/Portefeuille AM.csv')

    print("     ~ Get id_soc unique in df_aw")
    societes = []
    for i in range(len(df_am)):
        societes.append(df_am['society_id'][i])


    import datetime
    debut_annee = datetime.date(2021, 10, 1)
    fin_annee = datetime.date.today().replace(day=1)
    mois_annee = [(mois.year, mois.month, f"{mois.year}-{mois.month:02d}") for mois in
                  pd.date_range(debut_annee, fin_annee, freq="MS")]

    colonnes = ['society_id','month','year','year_month']
    df = pd.DataFrame(columns=colonnes)

    print("     ~ Create month & Year for each id_soc")
    for societe in societes:
        for annee, mois, mois_annee_str in mois_annee:
            ligne = {"society_id": societe, "month": mois, "year": annee,
                     "year_month": mois_annee_str}
            df = df._append(ligne, ignore_index=True)
    df.to_csv('referentiel_month_2021.csv', index=False)


    print("     ~ Create column name_org")
    l_name = []
    for i in range(len(df)):
        id_soc = df['society_id'][i]
        s_name = df_am.loc[df_am['society_id'] == id_soc, "name_org"].values[0]
        l_name.append(s_name)
    df['name_org'] = l_name

    df.to_csv('referentiel_month_2021.csv', index=False)

    print("** End : Create referential")

def unique_search(start_date,end_date):
    pipeline = [
        {
            "$match": {
                "createdAt": {"$gte": start_date},
                "type": {"$in": ["receipt", "unitary", "credit", "fees"]},
                "status": "paid",
            }
        },
        {
            "$group": {
                "_id": {
                    "society_id": "$societyId",
                    # "billingId": "$billingId",
                    "year_month": {"$dateToString": {"format": "%Y-%m", "date": "$createdAt"}},
                        "year": {"$dateToString": {"format": "%Y", "date": "$createdAt"}},
                        "month": {"$dateToString": {"format": "%m", "date": "$createdAt"}},

                },
                "price_amount": {"$sum": "$price.amount"},
                # "billingId": {"$first": "$billingId"},
                # "itemId": {"$first": "$lines.itemId"},
                # "itemId_type": {"$first": "$lines.type"},
                "society_id": {"$first": "$societyId"},
                "last_resa": {"$last": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}

            }
        },
        {
            "$project": {
                "_id": 0,
                "society_id": 1,
                # "billingId": 1,
                # "itemId": 1,
                # "itemId_type": 1,
                "price_amount": 1,
                "year_month" : "$_id.year_month",
                "month": "$_id.month",
                "year": "$_id.year",
                "last_resa": 1
            }
        },
    ]

    result = col_bills.aggregate(pipeline)
    result_list = list(result)
    df = pd.DataFrame(result_list)
    df.to_excel("res.xlsx")
    df.to_csv("res.csv")

    df_am = pd.read_csv('results/Portefeuille AM.csv')

    l_s = []
    for i in range(len(df)):
        soc_id = df['society_id'][i]
        s = df_am.loc[df_am['society_id'] == soc_id, "name_org"].values[0]
        l_s.append(s)
    df['name_org'] = l_s
    df = df.sort_values(by=['name_org','year_month'])
    df.to_csv("results/res.csv")
    print("fichier res.csv crée")

def societies():
    print("* Start - Get Data from DataBase Mongo")

    print("     ~ In progress - Get data in Societies")

    cursor_soc = col_soc.find({})
    l_name_org, l_id_org,l_priceNormalFeeDefault,l_priceExtremFeeDefault,l_salesName,l_sub_price,l_created,l_members= [],[], [],[],[],[],[],[]
    for s in cursor_soc:
        name_org = (s['name'])
        id_org = (s['_id'])
        createdAt = (s['createdAt'])
        priceNormalFeeDefault= (s['priceNormalFeeDefault'])
        priceExtremFeeDefault = (s['priceExtremFeeDefault'])
        sub_price = (s['sub_price'])
        members = len(s['members'])

        l_name_org.append(name_org)
        l_id_org.append(id_org)
        l_created.append(createdAt)
        l_priceExtremFeeDefault.append(priceNormalFeeDefault)
        l_priceExtremFeeDefault.append(priceExtremFeeDefault)
        l_sub_price.append(sub_price)
        l_members.append(members)
    df_org = pd.DataFrame(list(zip(l_id_org, l_name_org, l_created, l_priceExtremFeeDefault,l_priceExtremFeeDefault,l_sub_price,l_members)),
        columns=['society_id', 'name_org', 'created', 'fee_default', 'fee_max', 'sub_price', 'members'])
    df_org['name_org'] = df_org['name_org'].replace('+Simple', 'Plus Simple')

    df_org.to_csv(f'results/extract_soc.csv')
    print("* End - Creation du fichier extract_soc")

def create_miss_month():
    print("** Start : Merge conso with referential & portefeuille")

    df_conso = pd.read_csv(f'results/res.csv')
    df_ref = pd.read_csv('referentiel_month_2021.csv')
    df_am = pd.read_csv('results/Portefeuille AM.csv')

    print("     ~ Get active info & name_org")
    l_act = []
    l_name = []

    for i in range(len(df_conso)):
        id_soc = df_conso['society_id'][i]
        s_active = df_am.loc[df_am['society_id'] == id_soc,'Inactif'].values[0]
        l_act.append(s_active)
        s_name = df_am.loc[df_am['society_id'] == id_soc,"name_org"].values[0]
        l_name.append(s_name)
    df_conso['name_org'] = l_name
    df_conso['Inactif'] = l_act

    print("     ~ Fill 0 if month empty")
    df_conso = pd.merge(df_ref, df_conso, on=['society_id','name_org','year_month'], how='outer')
    df_conso = df_conso.fillna(0)
    df_conso = df_conso[['society_id', 'name_org','month_x', 'year_x', 'year_month', 'last_resa','price_amount','Inactif']]
    df_conso = df_conso.rename(columns={'month_x': 'month', 'year_x': 'year'})

    print("     ~ Get AW")
    l_aw = []
    for i in range(len(df_conso)):
        id_soc = df_conso['society_id'][i]
        s_aw = df_am.loc[df_am['society_id'] == id_soc, "AW annuel"].values[0]
        l_aw.append(s_aw)
    df_conso['AW annuel'] = l_aw
    df_conso['AW annuel'] = df_conso['AW annuel'].astype(str).str.replace(' ', '')
    df_conso['AW annuel'] = df_conso['AW annuel'].astype(float)

    print("* End - Creation du fichier extract_conso avec les mois à 0 si vide")
    df_conso.to_csv('results/extract_conso.csv')

def last_conso():
    print("** Start : Get Last month resa")

    df_reel = pd.read_csv('results/extract_conso.csv')
    df_reel['last_resa'] = df_reel['last_resa'].replace("0","1900-01-01")
    # df_reel = df_reel[df_reel['last_resa'] != '0'].reset_index()
    df_reel['last_resa'] = pd.to_datetime(df_reel['last_resa'])
    derniere_commande = df_reel.groupby('society_id')['last_resa'].agg('max').reset_index()
    import datetime
    now = datetime.datetime.now()
    current_month = now.strftime('%Y-%m-%d')
    derniere_commande['last_resa_indays'] = (pd.to_datetime(current_month) - pd.to_datetime(derniere_commande['last_resa']))

    print("     ~ Merge last_conso with df_conso")
    df_reel = df_reel.fillna(0)

    df_reel = df_reel.groupby(['society_id','name_org',"month",'year','year_month','Inactif','AW annuel']).sum(numeric_only = True).reset_index()
    l_last, l_diff = [],[]
    for i in range(len(df_reel)):
        id_soc = df_reel['society_id'][i]
        s_last = derniere_commande.loc[derniere_commande['society_id'] == id_soc, 'last_resa'].values[0]
        l_last.append(s_last)
        s_diff = derniere_commande.loc[derniere_commande['society_id'] == id_soc, "last_resa_indays"].values[0]
        l_diff.append(s_diff)
    df_reel['last_resa'] = l_last
    df_reel['last_resa_indays'] = l_diff
    df_reel = df_reel.sort_values(by=['name_org','year_month'])

    df_reel.to_csv(f'results/reel_conso.csv')

    print("* End - Creation du fichier reel_conso avec le merge de last_conso")

def update_sheet():
    print(f"** Start : Upload Google Drive")

    print("     ~ Update files")

    df_conso = pd.read_csv("results/reel_conso.csv")
    df_soc = pd.read_csv("results/extract_soc.csv",index_col=False)
    df_am = pd.read_csv("results/Portefeuille AM.csv",index_col=False)

    df_conso['AW annuel'] = df_conso['AW annuel'].astype(str)
    df_conso['price_amount'] = df_conso['price_amount'].astype(str)
    df_conso['price_amount'] = [(df_conso['price_amount'][i].replace(".", ",")) for i in range(len(df_conso))]
    df_conso['AW annuel'] = [(df_conso['AW annuel'][i].replace(".", ",")) for i in range(len(df_conso))]

    df_reel = df_conso.fillna(0)
    l_inac = []
    for v in range(len(df_reel)):
        id_soc = (df_reel['society_id'][v])
        s = df_am.loc[df_am['society_id'] == id_soc,"Inactif"].values[0]
        l_inac.append(s)
    df_conso['Inactif'] = l_inac
    # df_conso = df_conso[(df_conso['Inactif'] == "Non")|(df_conso['Inactif'] == 0)].reset_index()

    df_conso['AW annuel'] = df_conso['AW annuel'].astype(str)
    df_conso['price_amount'] = df_conso['price_amount'].astype(str)
    df_conso['last_resa_indays'] = [(df_conso['last_resa_indays'][i].replace(" days","")) for i in range(len(df_conso))]
    df_conso['price_amount'] = [(df_conso['price_amount'][i].replace(".",",")) for i in range(len(df_conso))]
    df_conso['AW annuel'] = [(df_conso['AW annuel'][i].replace(".",",")) for i in range(len(df_conso))]
    df_conso.to_csv("results/reel_conso.csv")

    from oauth2client.service_account import ServiceAccountCredentials
    import gspread
    from gspread_pandas import Spread
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file",'https://spreadsheets.google.com/feeds',"https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds/creds_sheet.json", scope)

    client = gspread.authorize(creds)

    s = Spread('Conso_since_2021')
    s.df_to_sheet(df_soc, sheet='Info_soc', start='A1',replace=True)
    s.df_to_sheet(df_am, sheet='Portefeuille', start='A1',replace=True)
    s.df_to_sheet(df_conso, index=False, sheet='Conso_Month', start='A1', replace=True)

def notif():
    df = pd.read_csv("results/reel_conso.csv")
    print(len(df))
    df_20_50 = df[(df['last_resa_indays'] >= 20) & (df['last_resa_indays'] <= 49) & (df['Inactif'] == "Non") ]
    df_20_50 = df_20_50.drop_duplicates(subset="society_id").sort_values(by='last_resa_indays',ascending=False).reset_index()
    df_20_50 = df_20_50[['society_id','name_org','month','year','year_month','Inactif','AW annuel','price_amount','last_resa','last_resa_indays']]
    len_df_20_50 = (len(df_20_50))
    df_p50 = df[(df['last_resa_indays'] > 50) & (df['Inactif'] == "Non") ]
    df_p50 = df_p50.drop_duplicates(subset="society_id").sort_values(by='last_resa_indays',ascending=False).reset_index()
    df_p50 = df_p50[['society_id','name_org','month','year','year_month','Inactif','AW annuel','price_amount','last_resa','last_resa_indays']]
    len_df_p50 = (len(df_p50))

    df['year_month'] = pd.to_datetime(df['year_month'], format='%Y-%m')
    filtered_df = df[(df['year_month'] > pd.to_datetime('2022-9', format='%Y-%m')) & (df['Inactif'] == "Non")].reset_index()

    filtered_df['price_amount'] = filtered_df['price_amount'].str.replace(',', '.').astype(float)

    df_group = filtered_df.groupby(["society_id","name_org",'last_resa_indays','AW annuel']).sum(numeric_only=True).reset_index()
    df_group['AW annuel'] = df_group['AW annuel'].str.replace(',', '.').astype(float)

    df_group['ticketVSAW'] = (df_group['price_amount']/df_group['AW annuel']*100).round(2)
    print(len(df_group))

    df_group = df_group[df_group['ticketVSAW'] < 50]
    len_tickAw_m50 = (len(df_group))
    df_group.to_csv("res.csv")

    # import requests
    # import json
    # webhook_url = config.webhook_discord
    #
    # message_content = f"⚠️WARNING ACCOUNT ⚠️   " \
    #                   f"{len_df_20_50} comptes n'ont pas réservé depuis [20-50] jours " \
    #                   f"{len_df_p50} comptes n'ont pas réservé depuis plus de 50 jours. " \
    #                   f"{len_tickAw_m50} comptes ont -50% de ticketé vs awardé. " \
    #                   f"Checkez le dashboard"
    #
    #
    # data = {
    #     "content": message_content,
    #     "username" : "Bot Pat"
    # }
    #
    # json_data = json.dumps(data)
    # response = requests.post(webhook_url, data=json_data, headers={'Content-Type': 'application/json'})
    #
    # if response.status_code == 204:
    #     print("Message envoyé avec succès !")
    # else:
    #     print("Échec de l'envoi du message :", response.status_code, response.text)

def pipedrive():
    import requests
    import json

    url = f"https://api.pipedrive.com/v1/activities?api_token={config.api_pipedrive}"

    payload = json.dumps({
        "due_date": "2023-05-16",
        "org_id": 135504,
        "note": "Dernière résa il y a 10 jours",
        "subject": "❗Warning Conso❗ A contacter ASAP",
        "type": "🌞 Point Account",
        "user_id": 6969457 # Maud 14766484 ou Pagna 15232994
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)