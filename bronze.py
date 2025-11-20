import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time

pd.set_option("display.max_colwidth", 150)
pd.set_option("display.min_rows", 20)

def busca_titulos_tesouro_direto():
    url = 'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv'
    df = pd.read_csv(url, sep=';', decimal=',')
    df['Data_Vencimento'] = pd.to_datetime(df['Data Vencimento'], dayfirst=True)
    df['Data_Base'] = pd.to_datetime(df['Data Base'], dayfirst=True)
    multi_indice = pd.MultiIndex.from_frame(df.iloc[:,:3])
    df = df.set_index(multi_indice).iloc[:, 3:]
    return df

print("Baixando dados do Tesouro Direto...")
titulos = busca_titulos_tesouro_direto()
print(f"Total de registros: {len(titulos)}")

prefixado = titulos.loc[('Tesouro Prefixado')].copy()
prefixado['Tipo'] = "PRE-FIXADOS"

for i, row in prefixado.iterrows():
    ifor_val = datetime.now() - timedelta(hours=1, minutes=0)
    prefixado.at[i,'dt_update'] = ifor_val
    time.sleep(1/10000)

prefixado = prefixado.rename(columns={
    "Taxa Compra Manha": "CompraManha",
    "Taxa Venda Manha": "VendaManha",
    "PU Compra Manha": "PUCompraManha",
    "PU Venda Manha": "PUVendaManha",
    "PU Base Manha": "PUBaseManha"
})

print(f"Registros Pré-fixados: {len(prefixado)}")

ipca = titulos.loc[('Tesouro IPCA+')].copy()
ipca['Tipo'] = "IPCA"

for i, row in ipca.iterrows():
    ifor_val = datetime.now() - timedelta(hours=1, minutes=0)
    ipca.at[i,'dt_update'] = ifor_val
    time.sleep(1/10000)

ipca = ipca.rename(columns={
    "Taxa Compra Manha": "CompraManha",
    "Taxa Venda Manha": "VendaManha",
    "PU Compra Manha": "PUCompraManha",
    "PU Venda Manha": "PUVendaManha",
    "PU Base Manha": "PUBaseManha"
})

print(f"Registros IPCA: {len(ipca)}")

connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
engine = create_engine(connection_string)

print("Conectado ao PostgreSQL!")

print("Gravando dados IPCA no PostgreSQL...")
ipca.to_sql("dadostesouroipca", con=engine, if_exists="replace", index=False)
print(f"{len(ipca)} registros IPCA gravados na tabela dadostesouroipca")

print("\nGravando dados Pre-fixados no PostgreSQL...")
prefixado.to_sql("dadostesouropre", con=engine, if_exists="replace", index=False)
print(f"{len(prefixado)} registros Pre-fixados gravados na tabela dadostesouropre")

print("\nDados da camada Bronze carregados com sucesso!")
