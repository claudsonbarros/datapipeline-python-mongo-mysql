import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os 



def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host =host_name,
        user =user_name,
        password =pw
    )
    print(cnx)
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor, db_name):
    query = f"CREATE DATABASE IF NOT EXISTS {db_name};"
    cursor.execute(query)

def consulta_database(cursor,query):
    cursor.execute(query)
    lista_dados = [i for i in cursor]
    return lista_dados


def executa_script(cursor, query,tipo=None,dados=None):
    if tipo == 1:
        cursor.execute(query)
    else:
        cursor.executemany(query,dados)

def read_csv(path):
    df = pd.read_csv(path)
    return df

def add_product_data(cnx, cursor, df, db_name, tb_name):
    lista_dados = [tuple(row) for i,row in df.iterrows()]
    query = f"INSERT INTO {db_name}.{tb_name} VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

    cursor.executemany(query,lista_dados)
    cnx.commit()

def main():
    #Carrega variavei do arquivo .env
    load_dotenv()

    host =os.getenv("DB_HOST")
    user =os.getenv("DB_USERNAME")
    password =os.getenv("DB_PASSWORD")

    #cria conexão
    cnx =connect_mysql(host, user, password)
    
    #cria cursor
    cursor =create_cursor(cnx)

    #Define nome da base de dados e cria base de dados
    db_name = 'dbprodutos2'
    create_database(cursor,db_name)

    # show_databases
    s_showdatabase = "SHOW DATABASES;"
    data_bases = consulta_database(cursor, s_showdatabase)
    print('Databases: ',data_bases)

    cursor.execute(f"USE {db_name};")

    s_create_tb_livros ="""
        CREATE TABLE IF NOT EXISTS dbprodutos2.tb_livros2(
            id VARCHAR(100),
                    Produto VARCHAR(100),
                    Categoria_Produto VARCHAR(100),
                    Preco FLOAT(10,2),
                    Frete FLOAT(10,2),
                    Data_Compra DATE,
                    Vendedor VARCHAR(100),
                    Local_Compra VARCHAR(100),
                    Avaliacao_Compra INT,
                    Tipo_Pagamento VARCHAR(100),
                    Qntd_Parcelas INT,
                    Latitude FLOAT(10,2),
                    Longitude FLOAT(10,2),

                    PRIMARY KEY (id));
    """

    executa_script(cursor,s_create_tb_livros,1)
    cnx.commit()



    # show_tables
    s_showdatabase = "SHOW TABLES;"
    data_tables = consulta_database(cursor, s_showdatabase)
    print('tabelas: ',data_tables)

    #consome csv
    df_livros = read_csv("./data/tb_livros.csv")

    #salva dados em tabela 
    tb_name = 'tb_livros2'
    add_product_data(cnx, cursor, df_livros, db_name, tb_name)


    #fecha cursor
    cursor.close()
    #fecha conexão
    cnx.close()





if __name__ == '__main__':
    main()