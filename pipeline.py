from etl import pipeline_carregar_kpi_de_vendas_consoliado, Path

pasta: str = Path.cwd() / 'data'
format_saida: list = ["csv","parquet","xlsx"]

pipeline_carregar_kpi_de_vendas_consoliado(pasta, format_saida)


