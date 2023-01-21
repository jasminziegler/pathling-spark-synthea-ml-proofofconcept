#!/bin/python

appName = "Kafka, Spark and FHIR Data"
kafka_topic = "fhir.post-gateway-kdb"
master1 = "local[*]"
master2 = "spark://spark-master:7077"

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pathling import PathlingContext

if __name__ == "__main__":

    for master in [master1, master2]:

        print("\n\n########################################################")
        print("########################################################")
        print("\nRunning on Spark master: {}\n".format(master))
        print("########################################################")
        print("########################################################\n\n")

        try:
            spark = SparkSession \
                .builder \
                .appName(appName) \
                .master(master) \
                .getOrCreate()

            print("\n\n########################################################")
            print("\nSystem Info\n")
            print("########################################################\n\n")
            print("Java version: {}\n\n".format(spark._jvm.java.lang.Runtime.version().toString()))
            print("Pyspark version: {}\n\n".format(pyspark.__version__))
            print(spark.sparkContext.getConf().getAll())
            print("########################################################\n\n")


            df = spark \
                .readStream  \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka1:19092") \
                .option("subscribe", kafka_topic) \
                .option("startingOffsets", "earliest") \
                .load()

            df.printSchema()

            query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                    .writeStream \
                    .queryName("gettable")\
                    .format("memory")\
                    .start()

            # close connection after 30 seconds
            query.awaitTermination(30)

            kafka_data = spark.sql("select * from gettable")
            kafka_data.show()
            type(kafka_data)

            ptl = PathlingContext.create(spark)

            patients = ptl.encode_bundle(kafka_data.select("value"), 'Patient')
            encounter = ptl.encode_bundle(kafka_data.select("value"), 'Encounter')
            condition = ptl.encode_bundle(kafka_data.select("value"), 'Condition')

            # show values
            patients.show()
            encounter.show()
            condition.show()

            # join resources
            pt1 = patients.select("id", "birthDate", "gender", "address.postalCode")
            enc1 = encounter \
                .select(
                    col("id").alias("encounter_id"),
                    "subject.reference",
                    col("serviceType.coding.code").alias("servicetype_code"),
                    col("period.start").alias("period_start"),
                    col("period.end").alias("period_end")
                   ) \
                .withColumn("patient_id", regexp_replace("reference", "Patient/", ""))
            cd1 = condition \
                .select(
                    "id",
                    "encounter.reference",
                    col("code.coding.code").alias("condition_code")
                   ) \
                .withColumn("cond_encounter_id", regexp_replace("reference", "Encounter/", ""))

            join1 = enc1 \
                .join(pt1, enc1.patient_id == pt1.id)
            join2 = join1 \
                .select("encounter_id", "servicetype_code", "period_start", "period_end", "patient_id", "birthDate", "gender", "postalCode") \
                .join(cd1, join1.encounter_id == cd1.cond_encounter_id) \
                .select("encounter_id", "servicetype_code", "period_start", "period_end", "patient_id", "birthDate", "gender", "postalCode", "condition_code")

            join2.show()

        except Exception as e:
            print(e)

        # finally stop spark session
        spark.stop()
