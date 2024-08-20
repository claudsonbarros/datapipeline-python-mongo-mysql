import requests 
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

def extract_api_data(url):
    response = requests.get(url)
    return response

def insert_data(col, data):
    docs = col.insert_many(data.json())
    return docs



def __main__():
    #Carrega variavei do arquivo .env
    load_dotenv()

    #Define Variaveis
    uri = os.getenv("MONGODB_URI")
    db_name = "db_produtos"
    col_name = "produtos"
    url = "https://labdados.com/produtos"

    #Cria conecão com a base de dados e retorna o client
    client = connect_mongo(uri)
    #cria base de dados
    db = create_connect_db(client,db_name)
    #cria coleção
    collection = create_connect_collection(db, col_name)

    #extrair os dados da API
    response = extract_api_data(url)
    data = len(response.json())
    print(f"\nQuantidade de dados extraidos: {data}")

    #inserir os dados da API na coleção criada.
    docs = insert_data(collection,response)
    n_docs = len(docs.inserted_ids)
    print(f"\nDocumentos inseridos na colecao: {n_docs}")


    client.close()
    
    
if __name__ == "__main__":
    __main__()