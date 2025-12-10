from pymongo.mongo_client import MongoClient
import json
import os

MONGO_URI = "mongodb+srv://etl_user_mongo:admin@cluster0.ug5qaw6.mongodb.net/?appName=Cluster0"
JSON_FILE = 'credit-card2.json'
DB_NAME = 'dados_fraude_nosql'
COLLECTION_NAME = 'transacoes_2'

if os.path.exists(JSON_FILE):
    try:
        client = MongoClient(MONGO_URI)
        
        with open(JSON_FILE, 'r') as f:
            data_list = json.load(f) 
        
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        collection.insert_many(data_list)
        
        print(f"Ingestão no MongoDB Atlas concluída: {len(data_list)} documentos carregados em '{COLLECTION_NAME}'.")
    except Exception as e:
        print(f"Erro na ingestão do MongoDB Atlas. Verifique a URI ou o Acesso IP. Erro: {e}")
else:
    print(f"Arquivo {JSON_FILE} não encontrado na pasta home.")