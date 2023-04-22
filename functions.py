import pandas as pd
from pymongo import MongoClient
import config
from datetime import datetime

client = MongoClient(f'mongodb+srv://{config.mongo_pat}')
db = client['legacy-api-management']
col_soc = db["societies"]
col_it = db["items"]


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
    debut_annee = datetime.date(2021, 1, 1)
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

def resa(start_date,end_date):
    print("* Start - Get Data from DataBase Mongo")

    # Création de la requête de recherche
    query = {
        'createdAt': {
            '$gte': start_date,
            '$lte': end_date
        }
    }

    # Création de la requête d'agrégation
    pipeline = [
        {'$match': query},
        {'$group': {
            '_id': {
                'id_soc': '$society._id',
                'type': '$type',
                'year_month': {'$dateToString': {'format': '%Y-%m', 'date': '$createdAt'}},
                'year': {'$dateToString': {'format': '%Y', 'date': '$createdAt'}},
                'month': {'$dateToString': {'format': '%m', 'date': '$createdAt'}}
            },
            'total': {'$sum': '$price.amount'}
        }},
        {'$sort': {'_id': 1}}
    ]

    # Exécution de la requête d'agrégation
    results = list(col_it.aggregate(pipeline))

    print("     ~ In progress - Stock Items in DF")

    # Création d'un DataFrame Pandas pour stocker les résultats
    df = pd.DataFrame(columns=['society_id', 'type', 'month','year','year_month','price_amount'])

    # Parcours des résultats et remplissage du DataFrame
    for result in results:
        id_soc = result['_id']['id_soc']
        item_type = result['_id']['type']
        year_month = result['_id']['year_month']
        month = result['_id']['month']
        year = result['_id']['year']
        total = result['total']
        df = df._append({'society_id': id_soc, 'type': item_type, 'month': month,'year':year, 'year_month': year_month, 'price_amount': total},
                       ignore_index=True)
    # Affichage du DataFrame
    df.to_csv(f'results/extract_conso.csv')

    # ### On recupère la liste des entreprises dans la base et on compare à la conso
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
    print("* End - Get Data from DataBase Mongo")

def create_miss_month():
    print("** Start : Merge conso with referential & portefeuille")

    df_conso = pd.read_csv(f'results/extract_conso.csv')
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
    print("     ~ Keep Active")
    df_conso = df_conso[df_conso['Inactif'] != "Oui"]
    df_conso = df_conso[["society_id",'name_org', "type", "month", "year", "year_month", "price_amount", "Inactif"]]
    df_conso.to_csv(f'results/extract_conso_type.csv')

    print("     ~ Fill 0 if month empty")
    df_conso = pd.merge(df_ref, df_conso, on=['society_id','name_org','year_month'], how='outer')
    df_conso = df_conso.fillna(0)
    df_conso = df_conso[['society_id', 'name_org','month_x', 'year_x', 'year_month', 'price_amount','Inactif']]
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

    print("** End : Merge conso with referential & portefeuille")
    df_conso.to_csv('results/reel_conso.csv')

def last_conso():
    print("** Start : Get Last month resa")

    df_conso = pd.read_csv('results/reel_conso.csv')

    print("     ~ Ignore conso = 0")
    grouped = df_conso.groupby('society_id')
    def last_non_zero_month(group):
        non_zero_mask = (group['price_amount'] != 0)
        if non_zero_mask.any():
            last_non_zero_index = non_zero_mask[::-1].idxmax()
            last_non_zero_row = group.loc[last_non_zero_index]
            return last_non_zero_row['year_month']
        else:
            return pd.NaT

    # Appliquer la fonction pour chaque groupe et concaténer les résultats
    last_months = grouped.apply(last_non_zero_month).reset_index(name='last_conso_month')
    print("     ~ Stock results")

    import datetime
    # Créer une variable contenant le mois actuel
    now = datetime.datetime.now()
    current_month = now.strftime('%Y-%m')
    last_months['diff_month'] = (pd.to_datetime(current_month) - pd.to_datetime(last_months['last_conso_month'])).dt.days // 30
    # last_months.to_csv(f'results/last_conso.csv')

    print("     ~ Merge last_conso with df_conso")
    l_last, l_diff = [],[]
    for i in range(len(df_conso)):
        id_soc = df_conso['society_id'][i]
        s_last = last_months.loc[last_months['society_id'] == id_soc, 'last_conso_month'].values[0]
        l_last.append(s_last)
        s_diff = last_months.loc[last_months['society_id'] == id_soc, "diff_month"].values[0]
        l_diff.append(s_diff)
    df_conso['last_conso_month'] = l_last
    df_conso['diff_month'] = l_diff
    df_conso = df_conso.groupby(['society_id','name_org','Inactif','month','year','year_month','last_conso_month',"diff_month",'AW annuel']).sum()
    df_conso.to_csv(f'results/reel_conso.csv')

    print("** End : Get Last month resa")


def update_sheet():
    print(f"** Start : Upload Google Drive")

    print("     ~ Update files")

    df_conso = pd.read_csv("results/reel_conso.csv",index_col=False)
    df_soc = pd.read_csv("results/extract_soc.csv",index_col=False)
    df_type = pd.read_csv("results/extract_conso.csv",index_col=False)
    df_am = pd.read_csv("results/Portefeuille AM.csv",index_col=False)
    df_conso = df_conso.fillna(0)
    df_conso['AW annuel'] = df_conso['AW annuel'].astype(float)


    from oauth2client.service_account import ServiceAccountCredentials
    import gspread
    from gspread_pandas import Spread
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds/creds_sheet.json", scope)

    client = gspread.authorize(creds)

    # gspread_pandas pour ajouter le df dans le sheet
    s = Spread('Conso_since_2021')
    s.df_to_sheet(df_conso, sheet='Conso_Month', start='A1',replace=True)
    s.df_to_sheet(df_soc, sheet='Info_soc', start='A1',replace=True)
    s.df_to_sheet(df_type, sheet='Conso_by_type', start='A1',replace=True)
    s.df_to_sheet(df_am, sheet='Portefeuille', start='A1',replace=True)

    sheet = client.open('Conso_since_2021').worksheet('Conso_Month')
    print("     ~ Convert column to float")

    i_values = sheet.col_values(9)
    j_values = sheet.col_values(10)
    l_values = sheet.col_values(12)

    i_float_values = [float(val) for val in i_values[1:]]
    j_float_values = [float(val) for val in j_values[1:]]
    l_float_values = [float(val) for val in l_values[1:]]

    cell_list = sheet.range('I2:I' + str(len(i_float_values) + 1)) + sheet.range('J2:J' + str(len(j_float_values) + 1) ) + sheet.range('L2:L' + str(len(l_float_values) + 1) )
    float_values = i_float_values + j_float_values + l_float_values

    for i, cell in enumerate(cell_list):
        cell.value = float_values[i]

    sheet.update_cells(cell_list)
    #######
    sheet = client.open('Conso_since_2021').worksheet('Conso_by_type')
    i_values = sheet.col_values(9)

    i_float_values = [float(val) for val in i_values[1:]]
    cell_list = sheet.range('I2:I' + str(len(i_float_values) + 1))
    float_values =i_float_values

    for i, cell in enumerate(cell_list):
        cell.value = float_values[i]

    sheet.update_cells(cell_list)

    print(f"** End : Upload Google Drive")

    ###########
