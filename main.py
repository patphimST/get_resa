from functions import *

### GET ALL
start_date = datetime(2021, 10, 1,0,0)
end_date = datetime(2023,10, 30, 23,59,59)
#
get_portefeuille()
referential_month()
societies()
#
unique_search(start_date)
#
create_miss_month()
last_conso()
update_sheet()

#### CREER DES ACTIVITES EN FONCTION DES WARNINGS ####
# notif()
# pipedrive()
########################################################

#### AFFICHE LES DATES DE DERNIERES RESA SUR LE PIPE ####
# get_last_resa()
# update_last_resa_pipe()
########################################################

# get_executive()
# all_items()