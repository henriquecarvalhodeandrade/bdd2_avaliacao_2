import pandas as pd
import os
import json

# Define o nome do arquivo de origem
FILE_ORIGIN = 'creditcard.csv'

# Verifica se o arquivo de origem existe
if not os.path.exists(FILE_ORIGIN):
    print(f"ERRO: Arquivo '{FILE_ORIGIN}' não encontrado no diretório atual.")
    exit()

try:
    # 1. Leitura do arquivo CSV original
    df_original = pd.read_csv(FILE_ORIGIN)
    total_rows = len(df_original)
    
    # 2. Cálculo do ponto médio (divisão inteira para garantir que o índice seja exato)
    mid_point = total_rows // 2
    
    # 3. Particionamento
    # df1: Primeira metade para MySQL
    df1_mysql = df_original.iloc[:mid_point]
    
    # df2: Segunda metade para MongoDB
    df2_mongo = df_original.iloc[mid_point:]
    
    # --- SALVAMENTO ---
    
    # 4. Salvar a Primeira Metade como CSV (para MySQL)
    file_mysql = 'credit-card1.csv'
    df1_mysql.to_csv(file_mysql, index=False)
    
    # 5. Transformar e Salvar a Segunda Metade como JSON (para MongoDB)
    file_mongo = 'credit-card2.json'
    
    # Usamos orient='records' que gera uma lista de objetos JSON, ideal para o MongoDB
    df2_mongo.to_json(file_mongo, orient='records') 
    
    print("\n✅ Particionamento concluído com sucesso!")
    print("---------------------------------------")
    print(f"Total de linhas no arquivo original: {total_rows}")
    print(f"-> {file_mysql} salvo: {len(df1_mysql)} linhas (para MySQL)")
    print(f"-> {file_mongo} salvo: {len(df2_mongo)} linhas (para MongoDB)")
    
except Exception as e:
    print(f"Ocorreu um erro durante o processamento: {e}")