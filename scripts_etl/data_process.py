from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, expr, struct, collect_list
import os

def create_country_name_mapping():
    mapping = {
        "ARG": "Argentina", "BRA": "Brasil", "CAN": "Canadá", "CHL": "Chile",
        "COL": "Colômbia", "HTI": "Haiti", "MEX": "México", "PER": "Peru",
        "PRY": "Paraguai", "URY": "Uruguai", "USA": "Estados Unidos",
        "EU28": "União Europeia (28)", "GBR": "Reino Unido", "NOR": "Noruega",
        "RUS": "Rússia", "CHE": "Suíça", "TUR": "Turquia", "UKR": "Ucrânia",
        "AUS": "Austrália", "BGD": "Bangladesh", "CHN": "China", "IND": "Índia",
        "IDN": "Indonésia", "IRN": "Irã", "ISR": "Israel", "JPN": "Japão",
        "KAZ": "Cazaquistão", "KOR": "Coreia do Sul", "MYS": "Malásia",
        "NZL": "Nova Zelândia", "PAK": "Paquistão", "PHL": "Filipinas",
        "SAU": "Arábia Saudita", "THA": "Tailândia", "VNM": "Vietnã",
        "DZA": "Argélia", "EGY": "Egito", "ETH": "Etiópia", "MOZ": "Moçambique",
        "NGA": "Nigéria", "SDN": "Sudão", "ZAF": "África do Sul",
        "TZA": "Tanzânia", "ZMB": "Zâmbia", "SSA": "África Subsaariana",
        "OECD": "OCDE", "WLD": "Mundo", "BRICS": "BRICS"
    }
    
    case_expression = "CASE "
    for code, name in mapping.items():
        case_expression += f"WHEN LOCATION = '{code}' THEN '{name}' "
    case_expression += "ELSE LOCATION END"
    return expr(case_expression)

def processar_dados_etl():
    spark_home = r'C:\spark-4.0.1-bin-hadoop3\spark-4.0.1-bin-hadoop3'
    os.environ['SPARK_HOME'] = spark_home
    os.environ['HADOOP_HOME'] = spark_home
    
    print("Iniciando sessão Spark (para ETL em Parquet)...")
    
    spark = SparkSession.builder \
        .appName("ETL_Consumo_Carne_Parquet") \
        .master("local[*]") \
        .getOrCreate() 

    path_data_raw = "data_raw/meat_consumption_worldwide.csv"
    path_data_processed = "data_processed/consumo_processado.parquet" 

    print(f"Carregando dados de {path_data_raw}...")
    df = spark.read.csv(path_data_raw, header=True, inferSchema=True)

    print("Adicionando nomes de países...")
    all_locations = df.select("LOCATION").distinct()
    map_expr_nome = create_country_name_mapping()
    df_nomes = all_locations.withColumn("Pais_Nome", map_expr_nome)
    df_com_nomes = df.join(df_nomes, "LOCATION", "left")

    print("Filtrando dados agregados...")
    agregados = ['WLD', 'OECD', 'BRICS', 'EU28', 'SSA']
    df_limpo = df_com_nomes.filter(~col("LOCATION").isin(agregados))

    print("Pivotando dados...")
    df_pivotado = df_limpo.groupBy("LOCATION", "Pais_Nome", "SUBJECT", "TIME") \
        .pivot("MEASURE", ["KG_CAP", "THND_TONNE"]) \
        .sum("Value") \
        .orderBy("LOCATION", "TIME")
    
    print("Schema final (plano) para o Parquet:")
    df_pivotado.printSchema()

    print(f"Salvando dados processados em {path_data_processed}...")
    df_pivotado.repartition(1).write \
        .mode("overwrite") \
        .parquet(path_data_processed)

    print("Processo ETL (Spark -> Parquet) concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    processar_dados_etl()
