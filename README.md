# 🥩 Consumo Mundial de Carne — Dashboard Interativo

Este repositório contém um projeto completo de **Data Analytics** desenvolvido para demonstrar uma **pipeline de ETL + Visualização de Dados**, utilizando **PySpark**, **MongoDB Atlas** e **Streamlit**.

O dashboard final permite analisar o consumo global de carne **Bovina, Suína, Aves e Ovinos**, com filtros por país, período e tipo de métrica (KG per Capita ou Milhares de Toneladas).


## Arquitetura e Fluxo de Dados

A solução segue uma arquitetura simples e robusta dividida em três etapas:

1. **Extração & Transformação (ETL):**
   Processamento do CSV bruto com Python e PySpark.

2. **Armazenamento (MongoDB – NoSQL):**
   Os dados transformados são enviados para o **MongoDB Atlas**, estruturados em formato aninhado e otimizado para consultas rápidas.

3. **Visualização (Streamlit):**
   O dashboard consome diretamente os dados do cluster MongoDB para gerar mapas, gráficos e KPIs em tempo real.

## Fonte dos Dados

Os dados brutos foram obtidos no Kaggle:

**Dataset:** *OECD - Meat Consumption Dataset*
**Arquivo original:** `oecd-meat-consumption.csv`
Link: [https://www.kaggle.com/datasets/sariag/consumption-of-meat-worldwide](https://www.kaggle.com/datasets/sariag/consumption-of-meat-worldwide)

O arquivo está localizado na pasta:

```
data_raw/
```


## Componentes do Repositório

| Arquivo/Pasta            | Função                                                             | Tecnologias                |
| ------------------------ | ------------------------------------------------------------------ | -------------------------- |
| `data_raw/`              | Armazena o CSV original vindo do Kaggle.                           | -                          |
| `data_process.py`        | Limpa, transforma e organiza os dados (join + pivot).              | PySpark, Pandas            |
| `load_mongo.py`          | Estrutura os dados de forma aninhada e envia para o MongoDB Atlas. | PyMongo, Pandas            |
| `app_com_comentarios.py` | Dashboard interativo com gráficos, KPIs e filtros dinâmicos.       | Streamlit, Plotly, PyMongo |
| `requirements.txt`       | Dependências do ambiente.                                          | -                          |
| `.gitignore`             | Garante que arquivos sensíveis e de cache não sejam versionados.   | -                          |



## ⚙️ Pré-requisitos e Configuração

### 1. Requisitos

* Python **3.8+**
* PySpark instalado
* Conta no **MongoDB Atlas**
* Connection String do seu cluster



### 2. Configuração do Ambiente

```bash
# Criar ambiente virtual
python -m venv venv

# Ativar (Windows)
.\venv\Scripts\activate

# Ativar (Linux/macOS)
source venv/bin/activate

# Instalar dependências
pip install -r requirements.txt
```


### 3. Configuração do MongoDB Atlas 

É indispensável a conexão no MongoDB Atlas, copie e cole em app.py a sua conexão de string:

#### Substitua no código:

```cmd
connection_string = "mongodb+srv://user:<password>..."
```


## Execução do Pipeline ETL

### 1. Processamento dos Dados (PySpark)

```bash
python data_process.py
```

Gera o arquivo:

```
data_processed/consumo_processado.parquet
```

### 2. Carga no MongoDB (PyMongo)

```bash
python load_mongo.py
```

Insere os dados na coleção:

```
dados_processados
```


## Iniciar o Dashboard

Com os dados no MongoDB, execute:

```bash
python -m streamlit run dashboard/app.py  
```

O dashboard será aberto no navegador (porta padrão: `8501`).


## 📊 Análises Disponíveis

* **Mapa Mundial (Choropleth):**
  Média do consumo por país, com opção para visualizar KG per Capita ou Mil Toneladas.

* **Evolução Temporal:**
  Gráficos de linha mostrando a tendência histórica por país.

* **Comparativo por Dieta:**
  Exibe a proporção de cada tipo de carne consumida.

* **Relatório Executivo:**
  KPIs como maior consumidor per capita e variação percentual ao longo do período.

