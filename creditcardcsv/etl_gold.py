
import pandas as pd
from sqlalchemy import create_engine
import os
import subprocess 


SILVER_PATH = os.path.expanduser('~/data_lakehouse/silver/cleaned_data.csv')
GOLD_DB_PATH = os.path.expanduser('~/data_lakehouse/gold/curated_data.db')
TABLE_NAME = 'fraude_final_model' 

def load_to_gold():
    if not os.path.exists(SILVER_PATH):
        print(f"Erro: Arquivo da Camada Silver não encontrado em {SILVER_PATH}")
        return

    print("Iniciando carregamento da Camada Gold...")
    
    
    df_curated = pd.read_csv(SILVER_PATH)
    

    df_curated = df_curated.sort_values(by='Time', ascending=True)

   
    engine = create_engine(f'sqlite:///{GOLD_DB_PATH}')
    
    
    df_curated.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
    
    print(f"\nCamada Gold concluída. Dados curados salvos em SQLite.")
    print(f"Tabela '{TABLE_NAME}' criada no arquivo {GOLD_DB_PATH}.")
    print(f"Total de linhas carregadas: {len(df_curated)}")
    print(f"Total de colunas (Features + Target): {len(df_curated.columns)}")
    
    
    verify_sqlite(GOLD_DB_PATH, TABLE_NAME)

def verify_sqlite(db_path, table_name):
    print("\n--- Verificação com sqlite3 ---")
    
    
    sql_count = f"SELECT COUNT(*) FROM {table_name};"
    
    
    command = ['sqlite3', db_path, sql_count]
    
    try:
        
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        count = result.stdout.strip()
        print(f"Contagem de linhas na tabela '{table_name}' (via sqlite3): {count}")

    except subprocess.CalledProcessError as e:
        print(f"Erro ao verificar o SQLite. Verifique a instalação do sqlite3. Erro: {e.stderr}")
    except FileNotFoundError:
        print("Erro: O comando 'sqlite3' não foi encontrado. Verifique a instalação.")

if __name__ == "__main__":
    load_to_gold()