package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder()
    .appName("job-1")
    .master("local[2]")
    .getOrCreate

    val df_init = spark
    .read.option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .csv("./src/main/resources/codesPostaux.csv")

    val df = df_init
    .na.drop(Seq("Ligne_5"))
    .select(countDistinct("Nom_commune"))
    
    val df2 = df
    .select(countDistinct("Nom_commune"))

    df.printSchema()

    val df3 = df
    .withColumn("num_departement",col("Code_commune_INSEE")
    .substr(1,2))
    
    val df_new = df
    .drop("Ligne_5", "coordonnees_gps", "Libellé_d_acheminement")
    .orderBy("Code_postal")
    
    df_new.write.option("header",true)
    .csv("./src/main/resources/commune_et_departement.csv")

    val df5 = df_new
    .select("Nom_commune","num_departement")
    .where("num_departement == '02'")

    val df6 = df_new
    .groupBy("num_departement")
    .agg(countDistinct("Nom_commune"))
    .sort(desc("count(Nom_commune)"))
    .limit(1)
    .show()

  }


}

