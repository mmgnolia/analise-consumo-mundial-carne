import pandas as pd
from pymongo import MongoClient
import sys

def carregar_dados_mongo():
    path_parquet = "data_processed/consumo_processado.parquet"
    connection_string = "mongodb+srv://magnolia:dani2016@cluster0.v3meszt.mongodb.net/?appName=Cluster0"
    db_name = "consumo_carne"
    collection_name = "dados_processados"

    try:
        print(f"Lendo dados do Parquet em {path_parquet}...")
        df = pd.read_parquet(path_parquet)
    except Exception as e:
        print(f"Erro ao ler o arquivo Parquet: {e}")
        print("Certifique-se de que executou 'python scripts_etl/data_process.py' primeiro.")
        sys.exit(1)

    print("Iniciando transformação para o schema aninhado (Pandas)...")
    
    df = df.rename(columns={
        "SUBJECT": "tipo",
        "KG_CAP": "consumo_per_capita_kg",
        "THND_TONNE": "consumo_total_thnd_tonne",
        "TIME": "ano",
        "Pais_Nome": "pais"
    })
    
    documentos_mongo = []

    for location, group_pais in df.groupby("LOCATION"):
        
        registros_consumo = []
        
        for ano, group_ano in group_pais.groupby("ano"):
            carnes = group_ano[[
                "tipo", 
                "consumo_per_capita_kg", 
                "consumo_total_thnd_tonne"
            ]].to_dict('records')
            
            registros_consumo.append({
                "ano": int(ano), 
                "carnes": carnes
            })
            
        documento = {
            "_id": location,
            "pais": group_pais.iloc[0]['pais'], 
            "registros_consumo": registros_consumo
        }
        documentos_mongo.append(documento)

    print(f"Transformação concluída. {len(documentos_mongo)} documentos de países prontos.")

    try:
        print(f"Conectando ao MongoDB Atlas (Modo Simples)...")
        client = MongoClient(connection_string)
        
        db = client[db_name]
        collection = db[collection_name]
        
        print("Enviando 'ping' para o servidor...")
        db.command('ping')
        print("Conexão bem-sucedida.")
        
        print(f"Limpando a coleção '{collection_name}'...")
        collection.delete_many({}) 
        
        print(f"Inserindo {len(documentos_mongo)} novos documentos...")
        collection.insert_many(documentos_mongo)
        
        print("Carga no MongoDB concluída com sucesso!")
        client.close()
        
    except Exception as e:
        print(f"Erro ao conectar ou escrever no MongoDB: {e}")
        print("Verifique sua Connection String e se o seu IP está libertado no 'Network Access' do Atlas.")
        sys.exit(1)

if __name__ == "__main__":
    carregar_dados_mongo()