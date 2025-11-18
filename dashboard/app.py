import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np 
from pymongo import MongoClient

st.set_page_config(
    page_title="Consumo Mundial de Carne",
    page_icon="🥩",
    layout="wide"
)

ANO_PROJECAO_INICIA = 2018 

TRADUCAO_CARNES = {
    "POULTRY": "Aves",
    "BEEF": "Bovina",
    "PIG": "Suína",
    "SHEEP": "Ovinos"
}

EMOJIS_CARNE = {
    "Aves": "🐔",
    "Bovina": "🐮",
    "Suína": "🐷",
    "Ovinos": "🐑"
}

@st.cache_data(show_spinner="Conectando ao MongoDB Atlas...")
def carregar_dados():
    
    connection_string = ""

    try:
        client = MongoClient(connection_string)
        db = client["consumo_carne"]
        collection = db["dados_processados"]
        
        data = list(collection.find({}))
        
        if not data:
            st.error("Erro: A coleção 'dados_processados' no MongoDB está vazia.")
            st.error("O script 'scripts_etl/load_mongo.py' falhou ou ainda não foi executado.")
            st.stop()
        
        client.close()

        df_anos = pd.json_normalize(
            data,
            record_path=['registros_consumo'],
            meta=['_id', 'pais']
        )
        
        df_final_flat = pd.json_normalize(
            df_anos.to_dict('records'),
            record_path=['carnes'],
            meta=['_id', 'pais', 'ano']
        )

    except Exception as e:
        st.error(f"Erro ao conectar ou ler do MongoDB: {e}")
        st.error(f"Verifique sua Connection String (senha) e se o seu IP está libertado no 'Network Access' do Atlas.")
        st.stop()

    df = df_final_flat.rename(columns={
        "ano": "Ano",
        "_id": "Pais_Codigo",
        "tipo": "Tipo_Carne_EN",
        "consumo_per_capita_kg": "Consumo_KG_Capita",
        "consumo_total_thnd_tonne": "Consumo_Mil_Toneladas",
        "pais": "País"
    })
    
    df['Consumo_KG_Capita'] = df['Consumo_KG_Capita'].fillna(0)
    df['Consumo_Mil_Toneladas'] = df['Consumo_Mil_Toneladas'].fillna(0) 
    
    if 'País' not in df.columns:
        df['País'] = df['Pais_Codigo']
        
    df['Tipo_Carne'] = df['Tipo_Carne_EN'].map(TRADUCAO_CARNES).fillna(df['Tipo_Carne_EN'])
        
    return df

df_completo = carregar_dados()

st.sidebar.title("Filtros 🌎")

paises_pt = sorted(df_completo['País'].unique())
paises_selecionados_nomes = st.sidebar.multiselect(
    "1. Países para Análise:",
    help="Selecione os países que deseja analisar nas abas 'Análise de Evolução' e 'Comparativos'.",
    options=paises_pt,
    default=["Brasil", "Estados Unidos", "Austrália", "Argentina", "China", "Índia"]
)

ano_min = int(df_completo['Ano'].min())
ano_max = int(df_completo['Ano'].max())

intervalo_anos = st.sidebar.slider( 
    "2. Período de Análise:", 
    min_value=ano_min,
    max_value=ano_max,
    value=(ano_max - 5, ano_max)
)
start_ano, end_ano = intervalo_anos

if end_ano >= ANO_PROJECAO_INICIA:
    st.sidebar.warning(f"**Aviso:** O período selecionado (até {end_ano}) inclui **projeções**.")


tipos_carne_pt = sorted(df_completo['Tipo_Carne'].unique())
carne_selecionada_pt = st.sidebar.selectbox(
    "3. Tipo de Carne:",
    help="Filtro global para todo o dashboard.",
    options=tipos_carne_pt,
    index=tipos_carne_pt.index("Aves") if "Aves" in tipos_carne_pt else 0
)

metrica_selecionada_label = st.sidebar.radio(
    "4. Métrica de Análise:",
    help="Selecione a métrica que irá colorir o mapa e será usada em todos os gráficos e KPIs.",
    options=["KG por Capita", "Milhares de Toneladas"],
    format_func=lambda x: "KG p/ Capita" if x == "KG por Capita" else "Milhares de Toneladas"
)
coluna_metrica = "Consumo_KG_Capita" if metrica_selecionada_label == "KG por Capita" else "Consumo_Mil_Toneladas"

df_base_filtrado = df_completo[
    (df_completo['Ano'] >= start_ano) &
    (df_completo['Ano'] <= end_ano) &
    (df_completo['Tipo_Carne'] == carne_selecionada_pt)
]

df_base_paises = df_completo[
    (df_completo['País'].isin(paises_selecionados_nomes)) &
    (df_completo['Tipo_Carne'] == carne_selecionada_pt) &
    (df_completo['Ano'] >= start_ano) &
    (df_completo['Ano'] <= end_ano)
]

df_filtrado_mapa = df_base_filtrado \
    .groupby(['País', 'Pais_Codigo'], as_index=False) \
    .agg({
        'Consumo_KG_Capita': 'mean',
        'Consumo_Mil_Toneladas': 'mean'
    })

df_filtrado_top20 = df_filtrado_mapa.sort_values(by=coluna_metrica, ascending=False).head(20)

df_comp_carnes_bruto = df_completo[
    (df_completo['Ano'] >= start_ano) &
    (df_completo['Ano'] <= end_ano) &
    (df_completo['País'].isin(paises_selecionados_nomes))
]
df_filtrado_comp_carnes = df_comp_carnes_bruto \
    .groupby(['País', 'Tipo_Carne'], as_index=False) \
    .agg({
        'Consumo_KG_Capita': 'mean',
        'Consumo_Mil_Toneladas': 'mean'
    })

df_filtrado_evolucao = df_base_paises

df_filtrado_shift = df_completo[
    (df_completo['País'].isin(paises_selecionados_nomes)) &
    (df_completo['Tipo_Carne_EN'].isin(["POULTRY", "BEEF"])) &
    (df_completo['Ano'] >= start_ano) &
    (df_completo['Ano'] <= end_ano)
]

st.title("Dashboard de Consumo Mundial de Carne")

st.markdown(f"Analisando: **{carne_selecionada_pt} {EMOJIS_CARNE.get(carne_selecionada_pt, '🥩')}** | Métrica: **{metrica_selecionada_label}** | Período: **{start_ano} - {end_ano}**")

tab_mapa, tab_analise, tab_comparativo, tab_relatorio = st.tabs(
    ["🗺️ Mapa Mundial", "📈 Análise de Evolução", "📊 Comparativos", "📝 Relatório Executivo"]
)

with tab_mapa:
    st.header(f"Consumo Mundial (Média {start_ano}-{end_ano})")
    
    if end_ano >= ANO_PROJECAO_INICIA:
        st.warning(f"**Aviso:** Os dados exibidos (a partir de {ANO_PROJECAO_INICIA}) incluem **projeções**.")
    
    hover_data_config = { "Pais_Codigo": False }
    if coluna_metrica == "Consumo_KG_Capita":
        hover_data_config["Consumo_Mil_Toneladas"] = ':.0f'
        hover_data_config["Consumo_KG_Capita"] = ':.2f' 
    else:
        hover_data_config["Consumo_KG_Capita"] = ':.2f'
        hover_data_config["Consumo_Mil_Toneladas"] = ':.0f' 

    
    fig_mapa = px.choropleth(
        df_filtrado_mapa,
        locations="Pais_Codigo",
        locationmode="ISO-3",
        color=coluna_metrica, 
        hover_name="País",
        hover_data=hover_data_config, 
        color_continuous_scale=px.colors.sequential.YlOrRd,
        title=f"Média de Consumo de {carne_selecionada_pt}"
    )
    
    fig_mapa.update_layout(
        geo=dict(showframe=False, showcoastlines=False, projection_type='equirectangular'),
        margin={"r":0,"t":40,"l":0,"b":0}
    )
    
    st.plotly_chart(fig_mapa, use_container_width=True)
    
    st.info(
        "**Por que os mapas parecem tão diferentes?**\n\n"
        "- **KG p/ Capita:** Mostra a média por pessoa. Países com alta cultura de churrasco (ex: Argentina) se destacam.\n"
        "- **Milhares de Toneladas:** Mostra o volume total. Países com populações enormes (ex: EUA, China) se destacam."
    )

with tab_analise:
    st.header(f"Análise de Evolução Temporal ({start_ano}-{end_ano})")
    
    if not df_filtrado_evolucao.empty:
        fig_tendencia = px.line(
            df_filtrado_evolucao.sort_values(by="Ano"),
            x="Ano",
            y=coluna_metrica,
            color="País",
            title=f"Evolução do Consumo de {carne_selecionada_pt}"
        )
        
        if end_ano >= ANO_PROJECAO_INICIA:
            fig_tendencia.add_vrect(
                x0=max(start_ano, ANO_PROJECAO_INICIA - 0.5), 
                x1=end_ano + 0.5, 
                fillcolor="gray",
                opacity=0.1,
                line_width=0,
                annotation_text="Projeção",
                annotation_position="top left"
            )
        
        st.plotly_chart(fig_tendencia, use_container_width=True)
    else:
        st.info("Selecione pelo menos um país no Filtro 1 para ver a tendência.")

with tab_comparativo:
    st.header(f"Comparativos (Média do Período {start_ano}-{end_ano})") 
    
    if end_ano >= ANO_PROJECAO_INICIA:
        st.warning(f"**Aviso:** Os dados exibidos são a média do período selecionado, que inclui **projeções**.")
        
    st.subheader(f"Composição da Dieta (Perfil de Consumo) {EMOJIS_CARNE.get('Aves', '🐔')}{EMOJIS_CARNE.get('Bovina', '🐮')}{EMOJIS_CARNE.get('Suína', '🐷')}{EMOJIS_CARNE.get('Ovinos', '🐑')}")
    st.markdown(f"*(Média para: {', '.join(paises_selecionados_nomes)})*")
    
    if not df_filtrado_comp_carnes.empty:
        df_totals = df_filtrado_comp_carnes.groupby("País")["Consumo_KG_Capita"].sum().reset_index(name="Total_KG_Pais")
        df_comp_percent = df_filtrado_comp_carnes.merge(df_totals, on="País")
        
        df_comp_percent = df_comp_percent[df_comp_percent["Total_KG_Pais"] > 0]
        df_comp_percent["Participação"] = df_comp_percent["Consumo_KG_Capita"] / df_comp_percent["Total_KG_Pais"]

        fig_bar_carnes = px.bar(
            df_comp_percent,
            x="País",
            y="Participação", 
            color="Tipo_Carne",
            title=f"Participação de Cada Tipo de Carne no Consumo (Média KG p/ Capita)"
        )
        fig_bar_carnes.update_layout(yaxis_tickformat='.0%')
        st.plotly_chart(fig_bar_carnes, use_container_width=True)
    else:
        st.warning("Não há dados para esta combinação de filtros.")
    
    st.markdown("---")
    st.subheader(f"Top 20 Países Consumidores") 
    
    if not df_filtrado_top20.empty:
        fig_bar_paises = px.bar(
            df_filtrado_top20,
            x="País",
            y=coluna_metrica,
            title=f"Top 20 Países (Média de {metrica_selecionada_label}, {start_ano}-{end_ano})"
        )
        st.plotly_chart(fig_bar_paises, use_container_width=True)
    else:
        st.warning("Não há dados para esta combinação de filtros.")

with tab_relatorio:
    st.header("Relatório Executivo") 

    st.subheader(f"Panorama Global (Média {start_ano}-{end_ano})")
    if end_ano >= ANO_PROJECAO_INICIA:
        st.warning(f"**Aviso:** A análise global é baseada na média do período selecionado, que inclui **projeções**.")
        
    col_g1, col_g2 = st.columns(2)

    if not df_filtrado_mapa.empty:
        df_mapa_sem_duplicados = df_filtrado_mapa.drop_duplicates(subset=['Pais_Codigo'])
        
        top_capita_global = df_mapa_sem_duplicados.loc[df_mapa_sem_duplicados['Consumo_KG_Capita'].idxmax()]
        top_total_global = df_mapa_sem_duplicados.loc[df_mapa_sem_duplicados['Consumo_Mil_Toneladas'].idxmax()]

        with col_g1:
            st.metric(
                label="Maior Consumo Per Capita (Média)",
                value=f"{top_capita_global['País']} 🍖",
                delta=f"{top_capita_global['Consumo_KG_Capita']:.2f} kg"
            )
            st.markdown(f"*{top_capita_global['País']} lidera o consumo individual médio no período.*")
        
        with col_g2:
            st.metric(
                label="Maior Volume Total (Média)",
                value=f"{top_total_global['País']} 📦",
                delta=f"{top_total_global['Consumo_Mil_Toneladas']:,.0f} mil ton."
            )
            st.markdown(f"*{top_total_global['País']} é o maior mercado em volume médio no período.*")
    else:
        st.warning("Não há dados globais para esta combinação de filtros.")
    
    st.markdown("---") 
    st.subheader(f"Tendências de Crescimento de {carne_selecionada_pt} ({start_ano}-{end_ano})")
    st.markdown(f"Analisando a variação de **{metrica_selecionada_label}** para **{carne_selecionada_pt}** no grupo (`{', '.join(paises_selecionados_nomes)}`) entre {start_ano} e {end_ano}:")
    
    col_t1, col_t2, col_t3 = st.columns(3)
    cols_tendencia = [col_t1, col_t2, col_t3] 
    col_idx = 0

    if not paises_selecionados_nomes:
        st.warning("Selecione pelo menos um país no Filtro 1 para ver a análise de tendências.")
    else:
        for pais in paises_selecionados_nomes:
            df_pais_evolucao = df_base_paises[df_base_paises['País'] == pais].sort_values(by="Ano")
            
            if not df_pais_evolucao.empty:
                val_inicio = df_pais_evolucao.iloc[0][coluna_metrica]
                ano_inicio_real = df_pais_evolucao.iloc[0]['Ano']
                val_fim = df_pais_evolucao.iloc[-1][coluna_metrica]
                ano_fim_real = df_pais_evolucao.iloc[-1]['Ano']
                
                if val_inicio > 0 and ano_fim_real > ano_inicio_real:
                    crescimento = ((val_fim - val_inicio) / val_inicio) * 100
                    
                    if crescimento >= 0:
                        delta_str = f"+{crescimento:.1f}%"
                        delta_color = "normal" 
                    else:
                        delta_str = f"{crescimento:.1f}%"
                        delta_color = "inverse" 

                    with cols_tendencia[col_idx % 3]: 
                        st.metric(
                            label=f"Variação de {pais}",
                            value=f"{val_fim:.2f}",
                            delta=delta_str,
                            delta_color=delta_color
                        )
                        caption_text = f"De {val_inicio:.2f} ({ano_inicio_real}) para {val_fim:.2f} ({ano_fim_real})"
                        if ano_fim_real >= ANO_PROJECAO_INICIA:
                            caption_text += " *(inclui projeção)*"
                        st.caption(caption_text)
                else:
                     with cols_tendencia[col_idx % 3]:
                        st.metric(label=f"Variação de {pais}", value="N/A", delta="Sem dados suficientes", delta_color="off")
                        st.caption(f"Período muito curto ou dados insuficientes.")
                col_idx += 1
            else:
                with cols_tendencia[col_idx % 3]:
                    st.metric(label=f"Variação de {pais}", value="N/A", delta="Sem dados", delta_color="off")
                    st.caption(f"Nenhum dado neste período.")
                col_idx += 1