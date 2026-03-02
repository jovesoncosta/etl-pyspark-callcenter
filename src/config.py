from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
import os

#Define os caminhos de forma dinâmica para rodar em qualquer computador (Mac, Linux ou Windows)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_DATA_PATH = os.path.join(BASE_DIR, "data", "output", "vendas_diarias")

#Definir o schema explicitamente evita que o Spark perca tempo (e processamento)
SCHEMA_PESSOAS = StructType([
    StructField("Username", IntegerType(), True),
    StructField("Nome", StringType(), True),
    StructField("Função", StringType(), True),
    StructField("Operação", StringType(), True),
    StructField("Negócio", StringType(), True),
    StructField("Líder da Equipe", StringType(), True)
])

SCHEMA_TELEFONIA = StructType([
    StructField("ID_Ligacao", IntegerType(), True),
    StructField("Username", IntegerType(), True),
    StructField("ID_Cliente", IntegerType(), True),
    StructField("inicio_ligacao", TimestampType(), True),
    StructField("fim_ligação", TimestampType(), True),
    StructField("Motivo", StringType(), True),
    StructField("Valor venda", DoubleType(), True)
])

SCHEMA_AVALIACOES = StructType([
    StructField("Username", IntegerType(), True),
    StructField("Data", DateType(), True),
    StructField("ID_Monitoria", IntegerType(), True),
    StructField("Nota", DoubleType(), True)
])