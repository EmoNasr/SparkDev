package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class SparkWithSql {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("sparkSql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/db_hopital");
        options.put("user","root");
        options.put("password","");

        Dataset<Row> DTF1 = ss.read().format("jdbc")
                .options(options)
                .option("query","SELECT DATE(DATA_CONSULTATION) AS jour, COUNT(*) AS nb_consultation FROM consultation GROUP BY jour")
                .load();
        Dataset<Row> DTF2 = ss.read().format("jdbc")
                .options(options)
                .option("query","SELECT medecin.NOM, medecin.PRENOM, COUNT(*) AS nb_consultation FROM consultation INNER JOIN medecin ON consultation.ID_MEDECIN = medecin.ID GROUP BY medecin.ID")
                .load();
        Dataset<Row> DTF3 = ss.read().format("jdbc")
                .options(options)
                .option("query","SELECT medecin.NOM, medecin.PRENOM, COUNT(DISTINCT consultation.ID_PATIENT) AS nb_patients FROM consultation INNER JOIN medecin ON consultation.ID_MEDECIN = medecin.ID GROUP BY medecin.ID")
                .load();

        Dataset<Row> DTF4 = ss.read().format("jdbc")
                .options(options)
                .option("query","SELECT * FROM consultation")
                .load();
        DTF1.show();
        DTF2.show();
        DTF3.show();
        DTF4.show();


    }
}