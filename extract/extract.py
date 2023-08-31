from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from datetime import date


spark = SparkSession.builder.appName('Ingest').getOrCreate()
date_now = date.today()


def extractFiles(path, delimiter, schema):

  df = spark.read \
    .option("delimiter", delimiter) \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .schema(schema) \
    .csv(path)
  
  return df


def saveStorage (df, filename):

    df.repartition(1).write.option("header", True).option("encoding", "UTF-8").option("delimiter", ";").mode("overwrite").csv("gs://ingest-207c30567ab/raw/"+filename+'/'+str(date_now))

    return df

def main():
  
    schema_employee = StructType([
        StructField('employer_name',
                    StringType(), True),
        StructField('reviews_count',
                    IntegerType(), True),
        StructField('culture_count',
                    IntegerType(), True),
        StructField('salaries_count',
                    IntegerType(), True),
        StructField('benefits_count',
                    IntegerType(), True),
        StructField('employer_website',
                    StringType(), True),
        StructField('employer_headquarters',
                    StringType(), True),
        StructField('employer_founded',
                    FloatType(), True),
        StructField('employer_industry',
                    StringType(), True),
        StructField('employer_revenue',
                    StringType(), True),
        StructField('url',
                    StringType(), True),
        StructField('geral',
                    FloatType(), True),
        StructField('cultura_valores',
                    FloatType(), True),
        StructField('diversidade_inclusao', 
                FloatType(), True),
        StructField('qualidade_vida', 
                    FloatType(), True),
        StructField('alta_lideranca', 
                    FloatType(), True),
        StructField('remuneracao_beneficios', 
                    FloatType(), True),
        StructField('oportunidades_carreira', 
                    FloatType(), True),
        StructField('recomendam_outras_pessoas_perce', 
                    FloatType(), True),
        StructField('perspectiva_positiva_empresa_perce', 
                    FloatType(), True),
        StructField('segmento_banco', 
                    StringType(), True),
        StructField('nome_bank', 
                    StringType(), True),
        StructField('match_percent', 
                    IntegerType(), True)
        
    ])

    schema_banks =  StructType ([
        StructField('segmento',
                    StringType(), True),
        StructField('cnpj',
                    IntegerType(), True),
        StructField('nome',
                    StringType(), True)
    ])

    schema_complaint = StructType([
        StructField('ano',
                    StringType(), True),
        StructField('trimestre',
                    StringType(), True),
        StructField('categoria',
                    StringType(), True),
        StructField('tipo',
                    StringType(), True),
        StructField('cnpj_if',
                    StringType(), True),
        StructField('instituicao_financeira',
                    StringType(), True),
        StructField('indice',
                    FloatType(), True),
        StructField('quantidade_reclamacoes_reguladas_procedentes',
                    IntegerType(), True),
        StructField('quantidade_reclamacoes_reguladas_outras',
                    IntegerType(), True),
        StructField('quantidade_reclamacoes_nao_reguladas',
                    IntegerType(), True),
        StructField('quantidade_total_reclamacoes',
                    IntegerType(), True),
        StructField('quantidade_total_clientes_CCS_SCR',
                    IntegerType(), True),
        StructField('quantidade_clientes_CCS',
                    IntegerType(), True),
        StructField('quantidade_de_clientes_SCR',
                    IntegerType(), True)
    ])


    df_banks = extractFiles('gs://staging_ingest/Dados/Bancos/EnquadramentoInicia_v2.tsv/', '\t', schema_banks)
    saveStorage (df_banks, 'bank')

    df_employee = extractFiles('gs://staging_ingest/Dados/Empregados/*', '|', schema_employee)
    saveStorage (df_employee, 'employee')

    df_complaint = extractFiles('gs://staging_ingest/Dados/Reclamacoes/*', ';', schema_complaint)
    saveStorage (df_complaint, 'complaint')

  
if __name__ == "__main__":
  main()
