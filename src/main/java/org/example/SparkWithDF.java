package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class SparkWithDF {

    public static void main(String[] args) {


        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss = SparkSession.builder().master("local[*]").appName("sparkSql").getOrCreate();
        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://localhost:3306/db_hopital");
        options.put("user", "root");
        options.put("password", "");

        //        DFM = ss.read().format("jdbc").options(options).option("dbtable", "medecin").load();
        //        DFC = ss.read().format("jdbc").options(options).option("dbtable", "consultation").load();
        //        Dataset<Row> DFJ = DFM.join(DFC, DFC.col("id_medecin").equalTo(DFM.col("id")), "inner");
        //        DFJ.groupBy("nom", "prenom").count().show();
                ss.read()
                .format("jdbc")
                .options(options).option("query", "SELECT m.nom, m.prenom, COUNT(*) AS count FROM medecin m INNER JOIN consultation c ON m.id = c.id_medecin GROUP BY m.nom, m.prenom").load().show();

        //        DFC = ss.read()
        //              .format("jdbc")
        //              .options(options)
        //              .option("dbtable", "consultation")
        //              .load();
        //        DFM = ss.read()
        //              .format("jdbc")
        //              .options(options)
        //              .option("dbtable", "medecin")
        //              .load();
        //        Dataset<Row> DFJ1 = DFM.join(DFC, DFC.col("id_medecin")
        //                              .equalTo(DFM.col("id")), "inner");
        //        DFJ1.groupBy("nom", "prenom")
        //                  .agg(countDistinct("id_patient")
        //                  .as("nombre_patients"))
        //                  .show();
                ss.read()
                .format("jdbc")
                .options(options).option("query", "SELECT m.nom, m.prenom, COUNT(DISTINCT c.id_patient) AS nombre_patients FROM consultation c INNER JOIN medecin m ON c.id_medecin = m.id GROUP BY m.nom, m.prenom").load().show();
    }
}
