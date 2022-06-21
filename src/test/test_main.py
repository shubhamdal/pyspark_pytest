import sys
sys.path.insert(0, '../transform')
from src.transform.main import filter_spark_data_frame
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd


def test_filter_spark_data_frame_by_value():
  # Spark Context initialisation
  spark_context = SparkContext()
  sql_context = SQLContext(spark_context)

  # Input and output dataframes
  input=sql_context.read.csv('src/test/input_data.csv',header=True)

  expected_output=sql_context.read.csv('src/test/expected_output.csv',header=True)

  real_output = filter_spark_data_frame(input)
  real_output = get_sorted_data_frame(
      real_output.toPandas(),
      ['age', 'name'],
  )
  expected_output = get_sorted_data_frame(
      expected_output.toPandas(),
      ['age', 'name'],
  )

  # Equality assertion
  pd.testing.assert_frame_equal(
      expected_output,
      real_output,
      check_like=True,
  )
  # Close the Spark Context
  spark_context.stop()


def get_sorted_data_frame(data_frame, columns_list):
  return data_frame.sort_values(columns_list).reset_index(drop=True)