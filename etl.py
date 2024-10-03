import pandas as pd
from pathlib import Path
import os, glob

# Outra forma de buscar arquivos na pasta
# arquivos_json = glob.glob(os.path.join(pasta,'*.json'))

def extrair_dados(path: str) -> pd.DataFrame:
    arquivos_json = list(path.glob('*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json ]
    df_total = pd.concat(df_list,ignore_index=True)
    return df_total

def calcular_kpi_de_total_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Quantidade'] * df['Venda'] 
    return df

# Isso é considerado como procedure pq não há um return
# E sim um saida pra o csv ou parquet ou excel ou ate no banco
# é a descarga dos dados em algum lugar que geralmente é no fim do codigo
def carregar_dados(df: pd.DataFrame, format_saida: list):
    """
    parametro que vai ser ou "csv" ou "parket" ou os dois
    """
    for formato in format_saida:
        if formato == "csv":
            df.to_csv("dados.csv",index=False)
        if formato == "parquet":
            df.to_parquet("dados.parquet",index=False)
        if formato == "parquet":
            df.to_excel("dados.xlsx",index=False)    
                    
def pipeline_carregar_kpi_de_vendas_consoliado(pasta: str,format_saida: list):
    data_frame = extrair_dados(path=pasta)
    data_frame_calculado = calcular_kpi_de_total_vendas(df=data_frame)
    carregar_dados(data_frame_calculado,format_saida)
    
    

# if __name__ == "__main__":
#     pasta: str = Path.cwd() / 'data'
#     data_frame = extrair_dados(path=pasta)
#     data_frame_calculado = calcular_kpi_de_total_vendas(df=data_frame)
#     format_saida: list = ["csv","parquet","xlsx"]
#     df_fim = carregar_dados(data_frame_calculado,format_saida)
    