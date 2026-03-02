from pyspark.sql.functions import col, sum as _sum, avg as _avg, unix_timestamp, round as _round, desc
from src.config import RAW_DATA_PATH, SCHEMA_PESSOAS, SCHEMA_TELEFONIA, SCHEMA_AVALIACOES
from src.spark_utils import get_spark_session, get_logger

logger = get_logger(__name__)

def executar_analise():
    spark = get_spark_session("Analise_Exploratoria")
    
    logger.info("A carregar os dados...")
    df_pessoas = spark.read.csv(f"{RAW_DATA_PATH}/base_pessoas.csv", header=True, schema=SCHEMA_PESSOAS)
    df_telefonia = spark.read.csv(f"{RAW_DATA_PATH}/base_telefonia.csv", header=True, schema=SCHEMA_TELEFONIA)
    df_avaliacoes = spark.read.csv(f"{RAW_DATA_PATH}/base_avaliacoes.csv", header=True, schema=SCHEMA_AVALIACOES)

    logger.info("A calcular as métricas de vendas...")

    #Filtra apenas ligações com faturamento e usa o .cache() porque esse cruzamento 
    #Será reaproveitado em múltiplas consultas abaixo, economizando processamento.
    df_vendas = df_telefonia.filter(col("Motivo") == "Venda")
    df_vendas_pessoas = df_vendas.join(df_pessoas, on="Username", how="inner").cache()

    print("\n" + "="*50)
    print(" ANÁLISE SOBRE VENDAS")
    print("="*50)

    print("\n--- 1. Top 5 Vendedores (Maior Valor Total) ---")
    df_vendas_pessoas.groupBy("Nome") \
        .agg(_round(_sum("Valor venda"), 2).alias("Total_Vendas")) \
        .orderBy(desc("Total_Vendas")) \
        .limit(5).show(truncate=False)

    print("\n--- 2. Ticket Médio por Vendedor ---")
    df_vendas_pessoas.groupBy("Nome") \
        .agg(_round(_avg("Valor venda"), 2).alias("Ticket_Medio")) \
        .orderBy(desc("Ticket_Medio")) \
        .limit(10).show(truncate=False)

    ## CAMADA DE QUALIDADE DE DADOS (DATA QUALITY) ##
    logger.info("A aplicar regras de Qualidade de Dados no tempo das chamadas...")
    df_telefonia_duracao = df_telefonia.withColumn(
        "duracao_min", 
        (unix_timestamp("fim_ligação") - unix_timestamp("inicio_ligacao")) / 60
    )
    
    #Ignorando anomalias de sistema (viagens no tempo / valores negativos)
    df_telefonia_valida = df_telefonia_duracao.filter(col("duracao_min") >= 0)
    
    tempo_medio = df_telefonia_valida.select(_avg("duracao_min")).collect()[0][0]
    print(f"\n--- 3. Tempo Médio Real das Ligações: {tempo_medio:.2f} minutos ---")
    print("(Nota: Registos corrompidos com tempo negativo foram isolados pelo filtro de Data Quality)\n")

    print("\n" + "="*50)
    print(" ANÁLISE SOBRE AVALIAÇÕES")
    print("="*50)

    logger.info("A calcular as métricas de avaliações...")
    nota_media = df_avaliacoes.select(_avg("Nota")).collect()[0][0]
    print(f"\n--- 4. Nota Média Geral: {nota_media:.2f} ---\n")

    print("--- 5. Vendedor com Pior Média de Avaliação ---")
    df_avaliacoes.join(df_pessoas, on="Username", how="inner") \
        .groupBy("Nome") \
        .agg(_round(_avg("Nota"), 2).alias("Media_Nota")) \
        .orderBy("Media_Nota") \
        .limit(1).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    executar_analise()