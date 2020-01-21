import os

from pyspark.sql.functions import explode

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell'


def xmlToCsv(xml_file, spark):
    df = spark.read.format('com.databricks.spark.xml').options(rootTag='returns').options(rowTag='return').load(xml_file)
    print("Schema Before")
    df.printSchema()

    explode_df = df.withColumn("structCol", explode("returnItem")).selectExpr("accountingTypeCode", "parentPortId",
                                                                              "portId", "structCol.*")
    print("Schema After")
    explode_df.printSchema()

    return explode_df
