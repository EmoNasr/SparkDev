package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class SparkWithDF {

    public static void main(String[] args) {


        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss = SparkSession.builder().master("local[*]").appName("sparkSql").getOrCreate();
        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://localhost:3306/db_hopital");
        options.put("user", "root");
        options.put("password", "");


        Dataset<Row> datasetC = ss.read()
                .format("jdbc")
                .options(options)
                .option("dbtable", "consultation")
                .load();

        Dataset<Row> datasetM = ss.read()
                .format("jdbc")
                .options(options)
                .option("dbtable", "medecin")
                .load();

        Dataset<Row> datasetP = ss.read()
                .format("jdbc")
                .options(options)
                .option("dbtable", "patients")
                .load();

        datasetC.groupBy(datasetC.col("DATA_CONSULTATION"))
                .agg(countDistinct("id_patient").alias("ConsultationParJour"))
                .show();

        datasetM.join(datasetC,datasetC.col("id_medecin")
                        .equalTo(datasetM.col("id")),"inner")
                         .groupBy("nom","prenom")
                         .agg(countDistinct("id_patient").as("NombreDePatient"))
                     .show();

        datasetM.join(datasetC,datasetC.col("id_medecin")
                        .equalTo(datasetM.col("id")),"inner")
                .groupBy("nom","prenom")
                .count()
                .show();

    }
}
