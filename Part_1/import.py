import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# Connexion à la base PostgreSQL
engine = create_engine('postgresql://postgres:root@localhost:5432/Brief_23_alimenter_bdd')

# Création de la meta pour définir les tables
metadata = MetaData()

clients = Table('clients', metadata,
                Column('client_id', String, primary_key=True),
                Column('sex', String),
                Column('birth', String))

produits_sous_categorie = Table('produits_sous-categorie', metadata,
                                Column('product_id', String, primary_key=True),
                                Column('category', String),
                                Column('sub_category', String),
                                Column('price', Float),
                                Column('stock_quantity', Integer))

ventes = Table('ventes', metadata,
               Column('product_id', String),
               Column('date', String),
               Column('session_id', String),
               Column('client_id', String),
               Column('quantity_sold', Integer))

# Créer les tables dans la bdd
metadata.create_all(engine)

df_clients = pd.read_csv('data-csv/clients.csv', sep=';')
df_clients.to_sql('clients', con=engine, if_exists='replace', index=False)

df_produits = pd.read_csv('data-csv/produits_sous-categorie.csv', sep=';')
df_produits['price'] = df_produits['price'].str.replace(',', '.').astype(float)  # Correction du prix
df_produits.to_sql('produits_sous-categorie', con=engine, if_exists='replace', index=False)

df_ventes = pd.read_csv('data-csv/ventes.csv', sep=';')
df_ventes.to_sql('ventes', con=engine, if_exists='replace', index=False)

# Créer une session bdd
Session = sessionmaker(bind=engine)
session = Session()

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