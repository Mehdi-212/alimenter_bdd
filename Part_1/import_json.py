import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import os
import json

# Connexion à la base PostgreSQL
engine = create_engine('postgresql://postgres:root@localhost:5432/Brief_23_alimenter_bdd')

# Création de la meta pour définir les tables
metadata = MetaData()

clients_json = Table('clients_json', metadata,
                Column('client_id', String, primary_key=True),
                Column('sex', String),
                Column('birth', String))

produits_sous_categorie_json = Table('produits_sous-categorie_json', metadata,
                                Column('product_id', String, primary_key=True),
                                Column('category', String),
                                Column('sub_category', String),
                                Column('price', Float),
                                Column('stock_quantity', Integer))

ventes_json = Table('ventes_json', metadata,
               Column('product_id', String),
               Column('date', String),
               Column('session_id', String),
               Column('client_id', String),
               Column('quantity_sold', Integer))

# Créer les tables dans la bdd
metadata.create_all(engine)

df_clients = pd.read_csv('data-csv/clients.csv', sep=';')
df_clients.to_sql('clients_json', con=engine, if_exists='replace', index=False)

df_produits = pd.read_csv('data-csv/produits_sous-categorie.csv', sep=';')
df_produits['price'] = df_produits['price'].str.replace(',', '.').astype(float)  # Correction du prix
df_produits.to_sql('produits_sous-categorie_json', con=engine, if_exists='replace', index=False)

df_ventes = pd.read_csv('data-csv/ventes.csv', sep=';')
df_ventes.to_sql('ventes_json', con=engine, if_exists='replace', index=False)

json_files = {
    'clients.json': 'clients_json',
    'produits_sous-categorie.json': 'produits_sous-categorie_json',
    'ventes.json': 'ventes_json'
}

json_folder = 'data-json'

for file_name, table_name in json_files.items():
    file_path = os.path.join(json_folder, file_name)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            df_json = pd.json_normalize(data)
            
            if table_name == 'clients_json':
                df_json['birth'] = pd.to_numeric(df_json['birth'], errors='coerce', downcast='integer')
            if table_name == 'produits_sous-categorie_json':
                df_json['price'] = df_json['price'].str.replace(',', '.').astype(float)
                df_json['stock_quantity'] = pd.to_numeric(df_json['stock_quantity'], errors='coerce', downcast='integer')
            if table_name == 'ventes_json':
                df_json['quantity_sold'] = pd.to_numeric(df_json['quantity_sold'], errors='coerce', downcast='integer')
            
            # Importer les données JSON dans la table correspondante
            df_json.to_sql(table_name, con=engine, if_exists='replace', index=False)
    else:
        print(f"Le fichier {file_name} n'existe pas dans le dossier {json_folder}")

# Créer une session 
Session = sessionmaker(bind=engine)
session = Session()

print("Table clients_json:")
result_clients = session.execute(text('SELECT * FROM clients_json LIMIT 5')).fetchall()
for row in result_clients:
    print(row)

print("\nTable produits_sous-categorie_json:")
result_produits = session.execute(text('SELECT * FROM "produits_sous-categorie_json" LIMIT 5')).fetchall()
for row in result_produits:
    print(row)

print("\nTable ventes_json:")
result_ventes = session.execute(text('SELECT * FROM ventes_json LIMIT 5')).fetchall()
for row in result_ventes:
    print(row)

print("Table clients:")
result_clients = session.execute(text('SELECT * FROM clients LIMIT 5')).fetchall()
for row in result_clients:
    print(row)

print("\nTable produits_sous-categorie:")
result_produits = session.execute(text('SELECT * FROM "produits_sous-categorie" LIMIT 5')).fetchall()
for row in result_produits:
    print(row)

print("\nTable ventes:")
result_ventes = session.execute(text('SELECT * FROM ventes LIMIT 5')).fetchall()
for row in result_ventes:
    print(row)
