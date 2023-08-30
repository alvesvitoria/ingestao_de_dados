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
    .csv(path)
  
  return df


def transformBanks():
  banks = extractFiles('gs://ingest-207c30567ab/raw/bank/'+str(date_now)+'/part*',';')

  df_banks = banks.withColumn("Nome_banco", split("nome", " - ")[0]).withColumn("apagar", split("nome", " - ")[1])
  columns_to_drop = ["nome", "apagar"]
  df_banks = df_banks.select([col for col in df_banks.columns if col not in columns_to_drop])

  return df_banks


def tranformComplaint():

  complaint = extractFiles('gs://ingest-207c30567ab/raw/complaint/'+str(date_now)+'/part*', ';')
  df_complaint = complaint.withColumn("instituicao_financeira", split("instituicao_financeira", " \\(")[0])

  return df_complaint


def transformEmployee():
  df_employee = extractFiles('gs://ingest-207c30567ab/raw/employee/'+str(date_now)+'/part*', ';') 

  return df_employee


def joinDatasets ():
  
  df_banks = transformBanks()
  df_complaint = tranformComplaint()
  df_employee = transformEmployee()

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
      .option('table', 'spartan-rhino-396412.ingest.union_final') \
      .option("temporaryGcsBucket","ingest-207c30567ab") \
      .save()
    
  
def main():

  joinDatasets()
  loadDatasets()  


if __name__ == "__main__":
  main()