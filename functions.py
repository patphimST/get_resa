import time
import json

import pandas as pd
from pymongo import MongoClient
import config
import requests
from datetime import datetime
from bson.objectid import ObjectId

client = MongoClient(f'mongodb+srv://{config.mongo_pat}')
db = client['legacy-api-management']
col_soc = db["societies"]
col_it = db["items"]
col_bills = db["bills"]
col_users = db["users"]


def get_portefeuille():
    import requests

    # Remplacez ces valeurs par vos propres informations d'authentification Pipedrive
    FILTER_ID = 1289

    url = f"https://api.pipedrive.com/v1/organizations?filter_id={FILTER_ID}&limit=200&api_token={config.api_pipedrive}"

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
    for i in response:
        print(i)
        id_soc = (i['9d0760fac9b60ea2d3f590d3146d758735f2896d'])
        awarde = (i['446585f9020fe3190ca0fa5ef53fc429ef4b4441'])
        owner = (i['owner_id']['name'])
        name = (i['name'])
        l_society_id.append(id_soc)
        l_name.append(name)
        l_awarde.append(awarde)
        l_owner.append(owner)
    df = pd.DataFrame({'society_id': l_society_id, 'name_org': l_name,'awarde': l_awarde,'owner': l_owner,})
    df.to_csv("results/Portefeuille AM.csv")

def referential_month():
    print("** Start : Create referential")
    import pandas as pd

    # Créer un DataFrame avec toutes les combinaisons de mois et de sociétés.
    df_am = pd.read_csv(f'results/Portefeuille AM.csv')
    df_am = df_am.fillna(0)
    df_am = df_am[df_am['society_id'] != 0].reset_index()
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
        try:
            s_name = df_am.loc[df_am['society_id'] == id_soc, "name_org"].values[0]
        except:
            s_name = ""
        l_name.append(s_name)
    df['name_org'] = l_name

    df.to_csv('referentiel_month_2021.csv', index=False)

    print("** End : Create referential")

def unique_search(start_date):
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
                    "year_month": {"$dateToString": {"format": "%Y-%m", "date": "$createdAt"}},
                        "year": {"$dateToString": {"format": "%Y", "date": "$createdAt"}},
                        "month": {"$dateToString": {"format": "%m", "date": "$createdAt"}},

                },
                "price_amount": {"$sum": "$price.amount"},
                "society_id": {"$first": "$societyId"},
                "last_resa": {"$last": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}

            }
        },
        {
            "$project": {
                "_id": 0,
                "society_id": 1,
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
    df_am = df_am.fillna(0)
    df_am = df_am[df_am['society_id'] != 0].reset_index()

    df = df.fillna(0)
    df = df[df['society_id'] != 0].reset_index()
    
    l_s = []
    for i in range(len(df)):
        soc_id = df['society_id'][i]
        print(soc_id)
        try :
            s = df_am.loc[df_am['society_id'] == soc_id, "name_org"].values[0]
        except:
            s = 0
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
    df_conso = df_conso.fillna(0)
    df_conso = df_conso[df_conso['society_id'] != 0].reset_index()
    print("     ~ Get active info & name_org")
    l_act = []
    l_name = []

    for i in range(len(df_conso)):
        id_soc = df_conso['society_id'][i]
        # s_active = df_am.loc[df_am['society_id'] == id_soc,'Inactif'].values[0]
        # l_act.append(s_active)
        try:
            s_name = df_am.loc[df_am['society_id'] == id_soc,"name_org"].values[0]
        except:
            s_name = ""
        l_name.append(s_name)
    df_conso['name_org'] = l_name
    # df_conso['Inactif'] = l_act

    print("     ~ Fill 0 if month empty")
    df_conso = pd.merge(df_ref, df_conso, on=['society_id','name_org','year_month'], how='outer')
    df_conso = df_conso.fillna(0)
    df_conso = df_conso[['society_id', 'name_org','month_x', 'year_x', 'year_month', 'last_resa','price_amount']]
    df_conso = df_conso.rename(columns={'month_x': 'month', 'year_x': 'year'})

    print("     ~ Get AW")
    l_aw = []
    for i in range(len(df_conso)):
        id_soc = df_conso['society_id'][i]
        try:
            s_aw = df_am.loc[df_am['society_id'] == id_soc, "awarde"].values[0]
        except:
            s_aw = 0
        l_aw.append(s_aw)
    df_conso['AW annuel'] = l_aw
    df_conso['AW annuel'] = df_conso['AW annuel'].astype(str).str.replace(' ', '')
    # df_conso['AW annuel'] = df_conso['AW annuel'].str.replace(',00', '').str.replace(' €', '')
    # df_conso['AW annuel'] = df_conso['AW annuel'].astype(float)

    print("* End - Creation du fichier extract_conso avec les mois à 0 si vide")
    df_conso.to_csv('results/extract_conso.csv')

def last_conso():
    print("** Start : Get Last month resa")

    df_reel = pd.read_csv('results/extract_conso.csv')
    df_reel['last_resa'] = df_reel['last_resa'].replace("0","1900-01-01")
    df_reel['last_resa'] = pd.to_datetime(df_reel['last_resa'])
    derniere_commande = df_reel.groupby('society_id')['last_resa'].agg('max').reset_index()
    import datetime
    now = datetime.datetime.now()
    current_month = now.strftime('%Y-%m-%d')
    derniere_commande['last_resa_indays'] = (pd.to_datetime(current_month) - pd.to_datetime(derniere_commande['last_resa']))

    print("     ~ Merge last_conso with df_conso")
    df_reel = df_reel.fillna(0)

    df_reel = df_reel.groupby(['society_id','name_org',"month",'year','year_month','AW annuel']).sum(numeric_only = True).reset_index()
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

    # df_reel = df_conso.fillna(0)
    # l_inac = []
    # for v in range(len(df_reel)):
    #     id_soc = (df_reel['society_id'][v])
    #     s = df_am.loc[df_am['society_id'] == id_soc,"Inactif"].values[0]
    #     l_inac.append(s)
    # df_conso['Inactif'] = l_inac
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
    df_20_50 = df[(df['last_resa_indays'] >= 20) & (df['last_resa_indays'] <= 49) & (df['Inactif'] == "Non") ]
    df_20_50 = df_20_50.drop_duplicates(subset="society_id").sort_values(by='last_resa_indays',ascending=False).reset_index()
    df_20_50 = df_20_50[['society_id','name_org','month','year','year_month','Inactif','AW annuel','price_amount','last_resa','last_resa_indays']]
    len_df_20_50 = (len(df_20_50))
    df_20_50.to_csv('results/df_20_50.csv')

    df_p50 = df[(df['last_resa_indays'] > 50) & (df['Inactif'] == "Non")]
    df_p50 = df_p50.drop_duplicates(subset="society_id").sort_values(by='last_resa_indays',ascending=False).reset_index()
    df_p50 = df_p50[['society_id','name_org','month','year','year_month','Inactif','AW annuel','price_amount','last_resa','last_resa_indays']]
    len_df_p50 = (len(df_p50))
    df_p50.to_csv('results/df_p50.csv')

    df['year_month'] = pd.to_datetime(df['year_month'], format='%Y-%m')
    df = df.loc[(df['year_month'] > pd.to_datetime('2022-9', format='%Y-%m'))].reset_index()
    df['price_amount'] = df['price_amount'].str.replace(',', '.').astype(float)

    from datetime import datetime
    now = datetime.now()  # current date and time
    year = now.strftime("%Y")
    month = now.strftime("%m")
    this_year_month = year + "-" + month
    actif_this_month = df.loc[(df['year_month'] == this_year_month) & (df["price_amount"] != 0 ) & ((df["Inactif"] == "Non" )|(df["Inactif"] == "Ponctuel" )|(df["Inactif"] == "New" ))]
    len_actif_this_month = (len(actif_this_month))


    df_group = df.groupby(["society_id", "name_org", 'last_resa_indays', 'AW annuel','Inactif']).sum(numeric_only=True).reset_index()
    df_group['AW annuel'] = df_group['AW annuel'].str.replace(',', '.').astype(float)
    df_group['ticketVSAW'] = (df_group['price_amount'] / df_group['AW annuel'] * 100).round(2)
    df_group = df_group.loc[(df_group["Inactif"] == "Non" )]

    nulle= df_group[(df_group['ticketVSAW'] < 10)]
    nulle.to_csv('results/nulle.csv')

    tres_basse= df_group[(df_group['ticketVSAW'] >= 10) & (df_group['ticketVSAW'] <= 20) ]
    tres_basse.to_csv('results/tres_basse.csv')

    basse= df_group[(df_group['ticketVSAW'] >= 20)&(df_group['ticketVSAW'] <= 35)]
    basse.to_csv('results/basse.csv')

    tres_moderee= df_group[(df_group['ticketVSAW'] >= 35)&(df_group['ticketVSAW'] < 50)]
    moderee= df_group[(df_group['ticketVSAW'] >= 50)&(df_group['ticketVSAW'] < 80)]
    elevee= df_group[(df_group['ticketVSAW'] >= 80) & (df_group['ticketVSAW'] < 100)]
    tres_elevee= df_group[(df_group['ticketVSAW'] >= 80)]

    len_nulle = (len(nulle))
    len_tres_basse = (len(tres_basse))
    len_basse = (len(basse))
    len_moderee = (len(moderee))
    len_elevee = (len(elevee))
    len_tres_elevee = (len(tres_elevee))

    print("ici ", len_actif_this_month,len_df_20_50,len_df_p50,len_nulle,len_tres_basse,len_basse)
    # import requests
    # import json
    # webhook_url = config.webhook_discord
    #
    # message_content = f"⚠️WARNING ACCOUNT ⚠️   \n" \
    #                   f"Ce mois-ci, {len_actif_this_month} comptes ont réservé.\n" \
    #                   f"\n" \
    #                   f"{len_df_20_50} comptes n'ont pas réservé depuis [20-50] jours \n" \
    #                   f"{len_df_p50} comptes n'ont pas réservé depuis plus de 50 jours. \n" \
    #                   f"\n" \
    #                   f"{len_nulle} comptes ont moins de 10% de ticketé (vs awardé)\n" \
    #                   f"{len_tres_basse} comptes ont [10-20]% de ticketé (vs awardé)\n" \
    #                   f"{len_basse} comptes ont [20-50]% de ticketé (vs awardé)\n" \
    #                   f"{len_moderee} comptes ont [50-80]% de ticketé (vs awardé)\n" \
    #                   f"\n" \
    #                   f"Checkez le dashboard et vos activités Pipedrive"
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
    df_p50 = pd.read_csv("results/df_p50.csv")
    df_am = pd.read_csv("results/Portefeuille AM.csv")
    url = f"https://api.pipedrive.com/v1/activities?api_token={config.api_pipedrive}"

    from datetime import datetime, timedelta
    aujourd_hui = datetime.today()
    jour_semaine = aujourd_hui.weekday()  # Récupère le jour de la semaine (0 pour lundi, 1 pour mardi, ..., 6 pour dimanche)
    jours_jusqua_lundi = (0 - jour_semaine + 7) % 7  # Calcule le nombre de jours jusqu'au prochain lundi
    prochain_lundi = aujourd_hui + timedelta(days=jours_jusqua_lundi)
    prochain_lundi = prochain_lundi.strftime("%Y-%m-%d")
    print(prochain_lundi)
    l_score = []
    l_id = []
    l_name_org = []
    # GET ID FROM PORTEFEUILLE AM
    def search_org(df_am):
        for i in range(len(df_am)):
            name = df_am['name_org'][i]
            url = f"https://api.pipedrive.com/v1/organizations/search?term={name}&fields=name&api_token={config.api_pipedrive}"
            payload = {}
            headers = {
                'Accept': 'application/json',
                'Cookie': '__cf_bm=DS8G3oD5wwJ1qD_vn0elvhAOVTdS9w2POhpTl31v7GQ-1684848070-0-AZENKAbLX49CCDogV+1hs1JSdVF9PnW3q1URodkUIMCTUc0V2aMXFCxh5qaYWfqXGh4YBk0ODJ1XPlUeY8N84yU='
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            response = response.json()

            try:
                l_score.append(response['data']["items"][0]['result_score'])
                l_id.append(response['data']["items"][0]['item']['id'])
                l_name_org.append(response['data']["items"][0]['item']['name'])
            except:
                l_score.append("Not in Pipe")
                l_id.append("Not in Pipe")
                l_name_org.append("Not in Pipe")
        df_am['score_pipe'] = l_score
        df_am['name_pipe'] = l_name_org
        df_am['id_org'] = l_id
        df_am.to_csv('results/Portefeuille AM.csv')
    search_org(df_am)
    # for i in range(len(df_p50)):
    #     last_resa = df_p50['last_resa_indays'][i]
    #     payload = json.dumps({
    #         "due_date": f"{prochain_lundi}",
    #         "org_id": 135504,
    #         "note": f"A contacter ASAP",
    #         "subject": f" ⚠️Warning Conso ⚠️La dernière résa remonte à {last_resa} jours",
    #         "type": "🌞 Point Account",
    #         "user_id": 6969457 # Maud 14766484 ou Pagna 15232994
    #     })
    #     headers = {
    #         'Content-Type': 'application/json',
    #         'Accept': 'application/json',
    #     }
    #
    #     response = requests.request("POST", url, headers=headers, data=payload)
    #
    #     print(response.text)

def get_executive():

    executives = col_soc.find({"members.roles": "executive"})

    l_name = []
    l_id = []
    l_user = []
    l_postal = []
    l_city = []
    l_role = []

    for society in executives:
        for member in society["members"]:
            if "executive" in member["roles"]:
                l_user.append(member["user"])
                l_role.append(member["roles"])
                l_name.append(society["name"])
                l_id.append(society["_id"])
                l_city.append(society['address']['city'])
                try:
                    l_postal.append(society['address']['postal_code'])
                except:
                    l_postal.append("")
    df = pd.DataFrame(list(zip(l_id,l_name,l_city,l_postal,l_user,l_role)), columns=['society_id','name_org','city','postal','id_user','role']).sort_values(by='name_org')
    df = df[(df['name_org'] != "Supertripper")&(df['name_org'] != "supertripper")&(df['name_org'] != "perso")&(df['name_org'] != "Gabin ADMIN")]
    df = df.reset_index(drop=True)
    df.to_csv("exec.csv")
    df = pd.read_csv("exec.csv")

    l_firstname = []
    l_lastname = []
    l_email = []
    l_phone = []
    l_count = []
    l_username = []
    for d in range(len(df)):
        id_user = df['id_user'][d]
        count_info_user = col_users.count_documents({"_id": ObjectId(id_user)})
        l_count.append(count_info_user)
    df['count'] = l_count

    df = df[df['count'] != 0].reset_index()
    df.to_csv("exec.csv")

    for d in range(len(df)):
        id_user = df['id_user'][d]
        info_user = col_users.find({"_id": ObjectId(id_user)})
        for info in info_user:
            l_firstname.append(info['firstname'])
            l_lastname.append(info['lastname'])
            l_username.append(info['username'])
            l_email.append(info['email'])
            try:
                l_phone.append(info['phone'])
            except:
                l_phone.append("")
    df['username']=l_username
    df['firstname']=l_firstname
    df['lastname']=l_lastname
    df['email']=l_email
    df['phone']=l_phone

    df.to_csv(fr"C:\Users\Patrick\PycharmProjects\get_resa\resa3.csv")

def all_items():


    df_confirmed = pd.read_csv("activa.csv",delimiter=";")
    df_confirmed = df_confirmed.fillna("null")
    l_amount = []
    l_name = []
    l_startDate = []
    for c in range(len(df_confirmed)):
        id_car = (df_confirmed["itemId"][c])
        print(id_car)
        filter = {
                'society._id': ObjectId('5e5d320ff2f2ef001043b6a2'),
                'id': id_car
            }
        result = client['legacy-api-management']['items'].find(filter=filter)
        result_count = client['legacy-api-management']['items'].count_documents(filter=filter)

        if result_count == 1:
            for r in result:
                # l_amount.append(r['amount'])
                # l_startDate.append(r['startDate'])
                # l_name.append(r['name'])
                # print(r['amount'])
                l_amount.append(r['status'])
        else:
            l_amount.append("")
            # l_name.append("")
            # l_startDate.append("")

    # df_confirmed['startDate'] = l_startDate
    # df_confirmed['name'] = l_name
    df_confirmed['status'] = l_amount
    df_confirmed.to_csv('activass.csv')

def get_last_resa():
    import pymongo
    import pandas as pd

    pipeline = [
        {
            "$sort": {"createdAt": -1}
        },
        {
            "$group": {
                "_id": "$society._id",
                "latestCreatedAt": {"$first": "$createdAt"}
            }
        }
    ]
    results = list(col_it.aggregate(pipeline))

    df = pd.DataFrame(results)
    df.columns = ['society_id', 'latest_reservation_date']
    df.to_csv('results/last_resa2.csv')

def update_last_resa_pipe():
    df = pd.read_csv('results/last_resa2.csv')
    import requests
    l_id_pipe,l_warning = [],[]

    ## GET ID PIPE
    for i in range(len(df)):
        society_id = df['society_id'][i]
        url = f"https://api.pipedrive.com/v1/itemSearch/field?term={society_id}&field_type=organizationField&exact_match=false&field_key=9d0760fac9b60ea2d3f590d3146d758735f2896d&return_item_ids=true&api_token={config.api_pipedrive}"
        payload = {}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload).json()
        time.sleep(0.3)
        try:
            id_pipe = (response['data'][0]['id'])
        except:
            id_pipe = 0
        print(society_id,id_pipe)
        l_id_pipe.append(id_pipe)

    df['id_pipe'] = l_id_pipe

    ## DELTA RESA VS TODAY
    # df = df.fillna(0)
    df = df[df['id_pipe'] > 0].reset_index()

    date_du_jour = datetime.today().date()

    for i in range(len(df)):
        id_pipe = int(df['id_pipe'][i])
        autre_date = (df['latest_reservation_date'][i])
        format_str = "%Y-%m-%d %H:%M:%S.%f"
        autre_date = datetime.strptime(autre_date, format_str)
        autre_date = datetime.date(autre_date)
        difference_en_jours = (date_du_jour - autre_date).days
        l_warning.append(difference_en_jours)
        print(id_pipe)

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

        ## Créer une activité sur last resa supérieur à 20 jours
        ## Créer une activité sur awardé à la traine

    df['warning_conso2'] = l_warning
    df.to_csv('results/last_resa2.csv')