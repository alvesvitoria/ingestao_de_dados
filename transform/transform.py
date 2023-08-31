from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from pyspark.sql.functions import col, split, regexp_replace
from pyspark.sql.functions import mean, sum

spark = SparkSession.builder.appName('Ingest').getOrCreate()
date_now = date.today()

def extractFiles(path, delimiter):
  df = spark.read \
  .option("delimiter", delimiter) \
  .option("header", True) \
  .option("encoding", "UTF-8") \
  .parquet(path)
  
  return df


def joinDatasets ():
  
  df_banks = extractFiles('gs://ingest-207c30567ab/prepared/banks/'+str(date_now)+'/part*', ';')
  df_complaint = extractFiles('gs://ingest-207c30567ab/prepared/complaint/'+str(date_now)+'/part*', ';')
  df_employee = extractFiles('gs://ingest-207c30567ab/prepared/employee/'+str(date_now)+'/part*', ';')

  # Join DataFrames
  df_join1 = df_banks.join(df_employee, df_banks["Nome_banco"] == df_employee["nome_bank"], "inner")
  df_join1 = df_join1.drop("Nome_banco")

  df_join2 = df_banks.join(df_complaint, df_banks["Nome_banco"] == df_complaint["instituicao_financeira"], "inner")

  df_join2 = df_join2.groupby("cnpj").agg(
    mean(col("indice")).alias("indice_mean"),
    sum(col("quantidade_total_reclamacoes")).alias("Total_reclamações"),
    sum(col("quantidade_total_clientes_CCS_SCR")).alias("Total_clientes"))
  

  df_final = df_join1.join(df_join2, on=["CNPJ"], how="inner")

  return df_final


def loadDatasets():

  df2 = joinDatasets()

  # Save parquet
  df_select = df2.select("nome_bank","cnpj", "segmento", "Total_clientes", "indice_mean", "Total_reclamações", "geral", "remuneracao_beneficios")
  df_select.repartition(1).write.format("parquet").mode("overwrite").save("gs://ingest-207c30567ab/target/"+str(date_now))
  df_select.show()

  # Save into BQ
  df_select.write.format('bigquery') \
      .option('table', 'spartan-rhino-396412.ingest.union_final_ingest') \
      .option("temporaryGcsBucket","ingest-207c30567ab") \
      .save()


def main():
  
  joinDatasets()
  loadDatasets() 


if __name__ == "__main__":
  main()