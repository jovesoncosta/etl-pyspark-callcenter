import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date, round as _round
from datetime import datetime

#A fixture cria uma SparkSession isolada apenas para os testes (roda em memória)
@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("Testes_Unitarios_ETL") \
        .getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()

def test_etl_vendas_diarias_regras_negocio(spark):
    """Testa o filtro de vendas, agregação matemática e tratamento de nulos."""
    
    # Criar DataFrames de simulação (Mocks)
    dados_pessoas = [
        (1001, "Agente A", "Lider Alpha"),
        (1002, "Agente B", None)  #Simulando a falha de hierarquia (Líder Nulo)
    ]
    colunas_pessoas = ["Username", "Nome", "Líder da Equipe"]
    df_pessoas = spark.createDataFrame(dados_pessoas, colunas_pessoas)

    dados_telefonia = [
        #Agente A faz uma venda
        (1, 1001, datetime(2025, 3, 1, 10, 0), "Venda", 100.50),
        #Agente A faz uma reclamação (Deve ser ignorada pelo ETL)
        (2, 1001, datetime(2025, 3, 1, 11, 0), "Reclamação", 0.0),
        #Agente B faz uma venda num dia diferente
        (3, 1002, datetime(2025, 3, 2, 14, 0), "Venda", 250.75)
    ]
    colunas_telefonia = ["ID_Ligacao", "Username", "inicio_ligacao", "Motivo", "Valor venda"]
    df_telefonia = spark.createDataFrame(dados_telefonia, colunas_telefonia)

    #Ação (Act) Aplicar a exata mesma lógica do seu ETL (vendas_diarias.py)
    df_vendas = df_telefonia.filter(col("Motivo") == "Venda")
    
    df_resultado = df_vendas.join(df_pessoas, on="Username", how="inner") \
        .withColumn("Data_Venda", to_date(col("inicio_ligacao"))) \
        .groupBy("Líder da Equipe", "Data_Venda") \
        .agg(_round(_sum("Valor venda"), 2).alias("Total_Vendas_Diarias")) \
        .fillna({"Líder da Equipe": "Sem_Lider_Informado"})

    resultados_coletados = df_resultado.collect()

    #Validação (Assert) Confirmar se os resultados estão matematicamente corretos
    #O Dataframe final deve ter exatamente 2 linhas (Lider Alpha no dia 1 e Sem_Lider no dia 2)
    assert len(resultados_coletados) == 2, "O número de linhas agregadas está incorreto."

    #Extrair os valores para variáveis Python para facilitar a validação
    dict_resultados = {row["Líder da Equipe"]: row["Total_Vendas_Diarias"] for row in resultados_coletados}

    #Verifica se a reclamação de 0.0 foi ignorada e manteve apenas a venda de 100.50
    assert dict_resultados.get("Lider Alpha") == 100.50, "Erro na agregação ou no filtro do Motivo='Venda'."

    #Verifica se o nulo foi substituído e se o valor de 250.75 foi processado corretamente
    assert dict_resultados.get("Sem_Lider_Informado") == 250.75, "Erro no tratamento de Líderes Nulos."