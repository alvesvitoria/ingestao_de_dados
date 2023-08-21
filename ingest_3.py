from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime 

spark = SparkSession.builder.appName('Ingest 3').getOrCreate()
date_now = datetime.datetime.today()


def extractFiles(path, delimiter):

  df = spark.read \
    .option("delimiter", delimiter) \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .csv(path)
  
  return df


def transformBancos():
  bancos = extractFiles('gs://ingest-207c30567ab/raw_ingest/Bancos/EnquadramentoInicia_v2.tsv', '\t')

  df_bancos = bancos.withColumn("Nome", F.regexp_replace('Nome', '(-).*', '')) \
    .withColumn('Nome', F.regexp_replace('Nome', 'CR DITO', 'CRÉDITO')) \
    .withColumn('Nome', F.regexp_replace('Nome', 'ADMISS O', 'ADMISSÃO')) \
    .withColumn('Nome', F.regexp_replace('Nome', 'REGI O', 'REGIÃO')) \
    .withColumn('Nome', F.regexp_replace('Nome', 'SOLID RIA', 'SOLIDÁRIA'))

  return df_bancos


def tranformReclamacoes():

  reclamacoes = extractFiles('gs://ingest-207c30567ab/raw_ingest/Reclamacoes/', ';')

  df_reclamacoes = reclamacoes.withColumn("Instituição financeira", F.regexp_replace("Instituição financeira", "[(conglomerado)]", "")) 

  df_reclamacoes = df_reclamacoes.withColumnRenamed("CNPJ IF","CNPJ_IF")
  df_reclamacoes = df_reclamacoes.withColumnRenamed("Instituição financeira", "Instituicao_Financeira")


  return df_reclamacoes


def transformEmpregados():
  df_empregados = extractFiles('gs://ingest-207c30567ab/raw_ingest/Empregados/', '|')
  df_empregados = df_empregados.withColumnRenamed("Nome","Nome_Instituicao")
  df_empregados = df_empregados.withColumnRenamed("Segmento","Segmento_Banco")

  return df_empregados


def loadFile ():
  
  df_bancos = transformBancos()
  df_reclamacoes = tranformReclamacoes()

  # Joining Datasets
  inner_df = (
    df_bancos
    .join(df_reclamacoes, df_bancos.Nome == df_reclamacoes.Instituicao_Financeira, 'inner')
  )

  # Save CSV
  inner_df.write.option("header",True) \
    .csv("gs://ingest-207c30567ab/target/target_"+str(date_now))
  
  # Save into BQ
  inner_df.write.format('bigquery') \
    .option('table', 'spartan-rhino-396412.ingest.table_union') \
    .option("temporaryGcsBucket","ingest-207c30567ab") \
    .save()
  

def main():
  loadFile()
  
if __name__ == "__main__":
  main()