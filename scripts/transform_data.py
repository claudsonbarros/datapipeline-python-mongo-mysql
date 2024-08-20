import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

def visualize_collection(col):
    """
    imprime todos os documentos existentes na coleção.
    """
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    """
    renomeia uma coluna existente.
    """
    col.update_many({},{"$rename":{f"{col_name}":f"{new_name}"}})


def select_category(col, category):
    query = {"Categoria do Produto":f"{category}"}
    lista_livros = []
    
    for doc in col.find(query):
    
        lista_livros.append(doc)

    return lista_livros

def make_regex(col, regex):
    """
    seleciona documentos que correspondam a uma expressão regular específica.
    
    """
    query = {"Data da Compra":{"$regex":f"{regex}"}}
    lista_produtos = []
    for produto in col.find(query):
        lista_produtos.append(produto)
    return lista_produtos

def create_dataframe(lista):
    """
    Cria dataframe baseado em uma lista
    """
    df = pd.DataFrame(lista)
    return df

def format_date(df):
    """
    formata a coluna de datas do dataframe para o formato "ano-mes-dia".
    """
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"],format ="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")

def save_csv(df, path):
    """
    salva o dataframe como um arquivo CSV no caminho especificado.
    """
    df.to_csv(path,index=False )
    print(f"\nO Arquivo {path} foi salvo")


def main():
    #Carrega variavei do arquivo .env
    load_dotenv()

    #Define Variaveis
    uri = os.getenv("MONGODB_URI")
    db_name = "db_produtos"
    col_name = "produtos"

    #Cria conecão com a base de dados e retorna o client
    client = connect_mongo(uri)
    #cria base de dados
    db = create_connect_db(client,db_name)
    #cria coleção
    collection = create_connect_collection(db, col_name)

    #visualize_collection(collection)
    
    rename_column(collection, "lat", "Latitude")
    rename_column(collection, "lon", "Longitude")

    # salvando os dados da categoria livros
    lst_livros = select_category(collection, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros)
    save_csv(df_livros, "./data/tb_livros.csv")

    ## salvando os dados dos produtos vendidos a partir de 2021

    lst_produtos = make_regex(collection, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "./data/tb_produtos.csv")

    client.close()




if __name__ == "__main__":
    main()