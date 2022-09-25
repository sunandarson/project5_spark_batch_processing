from pyspark.sql import SparkSession
from pyspark import SparkContext


import pandas as pd
data = [['Scott', 50], ['Jeff', 45], ['Thomas', 54], ['Ann', 34]]

# Create the pandas DataFrame
pandasDF = pd.DataFrame(data, columns=['Name', 'Age'])


def spark_conn(app, config):
    master = config['ip']
    try:
        spark = SparkSession.builder \
            .master(master) \
            .appName(app) \
            .getOrCreate()

        print(f"[INFO] Success connect SPARK ENGINE .....")
        return spark
    except:
        print(f"[INFO] Success Can't SPARK ENGINE .....")


spark = spark_conn(
    'Testing', {"ip": "spark://BUKANHACKER.localdomain:7077"})

sparkDF = spark.createDataFrame(pandasDF)
da = sparkDF.toPandas()
