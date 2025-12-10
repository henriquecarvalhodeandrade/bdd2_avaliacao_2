import pandas as pd
import os

BRONZE_PATH = os.path.expanduser('~/data_lakehouse/bronze/raw_data.txt')
SILVER_PATH = os.path.expanduser('~/data_lakehouse/silver/cleaned_data.csv') 

def load_to_silver():
    if not os.path.exists(BRONZE_PATH):
        print(f"Erro: Arquivo da Camada Bronze não encontrado em {BRONZE_PATH}")
        return

   
    df = pd.read_csv(BRONZE_PATH, sep='|')
    
    
    features = ['Time', 'Amount', 'Class'] + [f'V{i}' for i in range(1, 29)]
    df_cleaned = df[features].copy()

    df_cleaned['Class'] = df_cleaned['Class'].astype(int)

    df_cleaned.to_csv(SILVER_PATH, index=False)
    
    print(f"Camada Silver concluída. Dados limpos salvos em: {SILVER_PATH}")
    print(f"Total de colunas na Silver: {len(df_cleaned.columns)}")

if __name__ == "__main__":
    load_to_silver()