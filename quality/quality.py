from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
import great_expectations as ge
from datetime import date

date_now = date.today()
spark = SparkSession.builder.appName('Quality').getOrCreate()


def qualityDatasets():

    df_quality = ge.read_parquet("gs://ingest-207c30567ab/target/"+str(date_now))

    # Verificando se os valores existem na coluna segmento
    df_quality.expect_column_values_to_be_in_set(
    "segmento",
    ["S1", "S2", "S3"])

    # Verificando se o campo existe
    df_quality.expect_column_to_exist('cnpj')

    # Verificando se existem valores null no campo cnpj
    df_quality.expect_column_values_to_not_be_null('cnpj')

    # Verificando se o campo cnpj é único
    df_quality.expect_column_values_to_be_unique('cnpj')

    df_quality.validate()

    df_quality.save_expectation_suite("gs://ingest-207c30567ab/target/"+str(date_now)+".json")


