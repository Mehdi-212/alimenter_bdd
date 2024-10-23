from pymongo import MongoClient
import json

# Co à MongoDB
client = MongoClient('localhost', 27017)
db = client['Alimenter_BDD']

collection_clients = db['clients']
collection_produits = db['produits_sous_categorie']
collection_ventes = db['ventes']


with open('../data-json/clients.json') as f:
    clients_data = json.load(f)

with open('../data-json/produits_sous-categorie.json') as f:
    produits_data = json.load(f)
    
with open('../data-json/ventes.json') as f:
    ventes_data = json.load(f)
    
collection_clients.insert_many(clients_data)
collection_produits.insert_many(produits_data)
collection_ventes.insert_many(ventes_data)