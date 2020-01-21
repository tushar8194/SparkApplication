from pyspark.sql import SparkSession
from util.TypeConversion import xmlToCsv


def init_spark():
    spark = SparkSession.builder.appName("XMLReader").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def main():
    spark, sc = init_spark()
    xml_file = "../resources/itemsales.xml"
    output_dir = "../resources/csv/"
    csv_df = xmlToCsv(xml_file, spark)
    csv_df.show()
    csv_df.write.mode("overwrite").option("header","true").option("delimiter",",").csv(output_dir)


if __name__ == '__main__':
    main()
