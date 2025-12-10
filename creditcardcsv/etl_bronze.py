import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
import json
import os

MYSQL_DB_URL = "mysql+pymysql://etl_user:admin@localhost:3306/dados_fraude"
MONGO_URI = "mongodb+srv://etl_user_mongo:admin@cluster0.ug5qaw6.mongodb.net/?appName=Cluster0"
OUTPUT_PATH = os.path.expanduser('~/data_lakehouse/bronze/raw_data.txt')

def extract_mysql():
    try:
        engine = create_engine(MYSQL_DB_URL)
        df_mysql = pd.read_sql('SELECT * FROM transacoes_1', con=engine)
        print(f"Dados do MySQL extraídos: {len(df_mysql)} linhas.")
        return df_mysql
    except Exception as e:
        print(f"Erro na extração do MySQL: {e}")
        return pd.DataFrame()

def extract_mongodb():
    try:
        client = MongoClient(MONGO_URI)
        db = client['dados_fraude_nosql']
        collection = db['transacoes_2']
        
        data = list(collection.find({}))
        
        df_mongo = pd.DataFrame(data)
        
        if '_id' in df_mongo.columns:
            df_mongo = df_mongo.drop(columns=['_id'])
            
        print(f"Dados do MongoDB Atlas extraídos: {len(df_mongo)} linhas.")
        return df_mongo
    except Exception as e:
        print(f"Erro na extração do MongoDB: {e}")
        return pd.DataFrame()

def load_to_bronze():
    df_mysql = extract_mysql()
    df_mongo = extract_mongodb()
    
    df_unified = pd.concat([df_mysql, df_mongo], ignore_index=True)

    df_unified.to_csv(OUTPUT_PATH, sep='|', index=False) 
    
    print(f"\nCamada Bronze concluída. Dados brutos salvos em {OUTPUT_PATH}.")
    print(f"Total de linhas na Bronze: {len(df_unified)}")

if __name__ == "__main__":
    load_to_bronze()