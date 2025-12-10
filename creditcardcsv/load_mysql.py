import pandas as pd
from sqlalchemy import create_engine
import os

MYSQL_DB_URL = "mysql+pymysql://etl_user:admin@localhost:3306/dados_fraude"
CSV_FILE = 'credit-card1.csv'
TABLE_NAME = 'transacoes_1'

if os.path.exists(CSV_FILE):
    try:
        engine = create_engine(MYSQL_DB_URL)
        df_mysql = pd.read_csv(CSV_FILE)
        
        df_mysql.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
        
        print(f"Ingestão no MySQL concluída: {len(df_mysql)} linhas carregadas em '{TABLE_NAME}'.")
    except Exception as e:
        print(f"Erro na ingestão do MySQL. Verifique o container ('docker ps') e a URL. Erro: {e}")
else:
    print(f"Arquivo {CSV_FILE} não encontrado na pasta home.")