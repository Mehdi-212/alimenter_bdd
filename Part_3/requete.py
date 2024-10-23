from pymongo import MongoClient
from sqlalchemy import create_engine, text

# co à MongoDB
client = MongoClient('localhost', 27017)
db = client['Alimenter_BDD']

collection_clients = db['clients']
collection_produits = db['produits_sous_categorie']
collection_ventes = db['ventes']

print("Clients de sexe féminin avec mongo (limité à 5 résultats) :")
for client in collection_clients.find({'sex': 'f'}).limit(5):
    print(client)

# Requête 2 : Limiter à 5 résultats pour les produits dans la catégorie 'Vêtements'
print("\nProduits dans la catégorie 'Vêtements' avec mongo (limité à 5 résultats) :")
for produit in collection_produits.find({'category': 'Vêtements'}).limit(5):
    print(produit)

# Requête 3 : Limiter à 5 résultats pour les ventes avec plus de 5 produits vendus
print("\nVentes avec plus de 5 produits vendus avec mongo (limité à 5 résultats) :")
for vente in collection_ventes.find({'quantity_sold': {'$gt': 5}}).limit(5):
    print(vente)


# Co à PostgreSQL
engine = create_engine('postgresql://postgres:root@localhost:5432/Brief_23_alimenter_bdd')

with engine.connect() as connection:
    print("\nClients avec PostgreSQL (limité à 5 résultats) :")
    result_clients = connection.execute(text("SELECT * FROM clients LIMIT 5"))
    for row in result_clients:
        print(row)

    print("\nProduits dans la catégorie 'Vêtements' avec PostgreSQL (limité à 5 résultats) :")
    result_produits = connection.execute(text('SELECT * FROM "produits_sous-categorie" WHERE category = \'Vêtements\' LIMIT 5'))
    for row in result_produits:
        print(row)

    print("\nVentes avec plus de 5 produits vendus avec PostgreSQL (limité à 5 résultats) :")
    result_ventes = connection.execute(text("SELECT * FROM ventes WHERE quantity_sold > 5 LIMIT 5"))
    for row in result_ventes:
        print(row)
