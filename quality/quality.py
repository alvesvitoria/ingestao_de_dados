import great_expectations as gx
import pandas as pd
from datetime import date
from pyspark.sql import SparkSession

date_now = date.today()

spark = SparkSession.builder.appName('Ingest').getOrCreate()

def process_ge():
  
  context = gx.get_context()


  datasource = context.sources.add_spark(name="spark_datasource")

  dataframe = spark.read.format("parquet").option("header", "true").load("gs://ingest-207c30567ab/prepared/banks/2023-08-31/part-00000-864c720b-cb1b-4ad4-b0fc-0a8ca22e11de-c000.snappy.parquet")

  name = "my_df_asset"
  data_asset = datasource.add_dataframe_asset(name=name)
  my_batch_request = data_asset.build_batch_request(dataframe=dataframe)


  context.add_or_update_expectation_suite("my_expectation_suite")

  validator = context.get_validator(
      batch_request=my_batch_request,
      expectation_suite_name="my_expectation_suite",
  )
  validator.head()

  return validator


def add_tests_suite(validator):

    validator.expect_column_values_to_not_be_null(column="cnpj")

    validator.expect_column_values_to_be_in_set(
    "segmento",
    ["S1", "S2", "S3", "S4", "S5"])

    validator.expect_column_values_to_be_unique('cnpj')

    validator.validate()
    
    config = validator.save_expectation_suite('gs://ingest-207c30567ab/quality/'+str(date_now)+'quality.json')

    return validator


def main ():
   
   dataset_validate = process_ge()

   add_tests_suite(dataset_validate)

  
if __name__ == "__main__":
  main()
