from pyspark.sql.functions import col, sum as _sum, to_date, round as _round
from src.config import RAW_DATA_PATH, OUTPUT_DATA_PATH, SCHEMA_PESSOAS, SCHEMA_TELEFONIA
from src.spark_utils import get_spark_session, get_logger

logger = get_logger(__name__)

def etl_vendas_diarias():
    spark = get_spark_session("ETL_Vendas_Diarias")
    
    logger.info("Iniciando extração...")
    df_pessoas = spark.read.csv(f"{RAW_DATA_PATH}/base_pessoas.csv", header=True, schema=SCHEMA_PESSOAS)
    df_telefonia = spark.read.csv(f"{RAW_DATA_PATH}/base_telefonia.csv", header=True, schema=SCHEMA_TELEFONIA)

    logger.info("Iniciando transformação...")

    #Considerar apenas ligações que efetivamente geraram vendas
    df_vendas = df_telefonia.filter(col("Motivo") == "Venda")
    
    df_agregado = df_vendas.join(df_pessoas, on="Username", how="inner") \
        .withColumn("Data_Venda", to_date(col("inicio_ligacao"))) \
        .groupBy("Líder da Equipe", "Data_Venda") \
        .agg(_round(_sum("Valor venda"), 2).alias("Total_Vendas_Diarias")) \
        .fillna({"Líder da Equipe": "Sem_Lider_Informado"})

    logger.info(f"Salvando dados em {OUTPUT_DATA_PATH}...")

    #repartition(1) resolve o problema de "Small Files" do Spark 
    #Garantindo que cada pasta de partição contenha apenas 1 arquivo limpo e consolidado.
    df_agregado.repartition(1) \
        .write \
        .partitionBy("Líder da Equipe") \
        .mode("overwrite") \
        .parquet(OUTPUT_DATA_PATH)

    logger.info("ETL concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    etl_vendas_diarias()