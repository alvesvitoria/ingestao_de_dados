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

def loadPrepared():
  df_banks = transformBanks()
  df_employee = transformEmployee()
  df_complaint = tranformComplaint()

  # Save parquet
  df_banks.repartition(1).write.format("parquet").mode("overwrite").save("gs://ingest-207c30567ab/prepared/banks/"+str(date_now))
  df_employee.repartition(1).write.format("parquet").mode("overwrite").save("gs://ingest-207c30567ab/prepared/employee/"+str(date_now))
  df_complaint.repartition(1).write.format("parquet").mode("overwrite").save("gs://ingest-207c30567ab/prepared/complaint/"+str(date_now))


def main():

  loadPrepared()


if __name__ == "__main__":
  main()