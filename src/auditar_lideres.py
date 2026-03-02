from pyspark.sql.functions import col, sum as _sum, round as _round, abs as _abs
from src.config import RAW_DATA_PATH, OUTPUT_DATA_PATH, SCHEMA_PESSOAS, SCHEMA_TELEFONIA
from src.spark_utils import get_spark_session, get_logger

logger = get_logger(__name__)

def auditar_vendas_por_lider():
    spark = get_spark_session("Auditor_Lideres")
    
    #QUE DEVERIA SER (Baseado nos CSVs brutos)
    logger.info("Calculando o total esperado por líder nos CSVs...")
    df_pessoas = spark.read.csv(f"{RAW_DATA_PATH}/base_pessoas.csv", header=True, schema=SCHEMA_PESSOAS)
    df_telefonia = spark.read.csv(f"{RAW_DATA_PATH}/base_telefonia.csv", header=True, schema=SCHEMA_TELEFONIA)
    
    #Faz o mesmo filtro e cruzamento
    df_vendas = df_telefonia.filter(col("Motivo") == "Venda")
    df_completo = df_vendas.join(df_pessoas, on="Username", how="inner") \
        .fillna({"Líder da Equipe": "Sem_Lider_Informado"})
        
    #Agrupa por líder e soma tudo
    df_esperado = df_completo.groupBy("Líder da Equipe") \
        .agg(_round(_sum("Valor venda"), 2).alias("Total_CSV"))
        
    #O QUE REALMENTE FOI SALVO (Lendo do Data Lake / Parquet)
    logger.info("Calculando o total real gravado nas pastas Parquet...")
    df_parquet = spark.read.parquet(OUTPUT_DATA_PATH)
    
    df_real = df_parquet.groupBy("Líder da Equipe") \
        .agg(_round(_sum("Total_Vendas_Diarias"), 2).alias("Total_Parquet"))

    #O CONFRONTO DIRETO (Join das duas agregações)
    logger.info("Confrontando os valores...")
    
    df_auditoria = df_esperado.join(df_real, on="Líder da Equipe", how="full_outer") \
        .withColumn("Diferença", _round(_abs(col("Total_CSV") - col("Total_Parquet")), 2))

    print("\n" + "="*70)
    print("AUDITORIA FINANCEIRA POR LÍDER (CSV vs PARQUET)")
    print("="*70)
    
    #Exibe a tabela comparativa no terminal
    df_auditoria.orderBy("Líder da Equipe").show(truncate=False)

    #Verifica se existe alguma linha onde a diferença não é zero
    erros = df_auditoria.filter(col("Diferença") > 0.0).count()
    
    if erros == 0:
        print("AUDITORIA APROVADA: Os valores batem 100% para TODOS os líderes.")
        print("Nenhuma venda foi trocada de pasta durante o particionamento.")
    else:
        print("ALERTA: Foram encontradas divergências nos valores de alguns líderes.")

    spark.stop()

if __name__ == "__main__":
    auditar_vendas_por_lider()