package com.autentia.tutoriales.ml.Examples;

import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.autentia.tutoriales.ml.context.SparkContext;
import com.autentia.tutoriales.ml.log.MensajeLogger;

public class PostgresExample {
    public static void main(String[] args) {
        MensajeLogger.mostrarMensaje("Apagar los logs no importantes de spark");
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        
        
    	SparkContext.inicializar();
    	
    	MensajeLogger.mostrarMensaje("Cargamos la base de datos");
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=example");
        options.put("dbtable", "beagle.alerts");
        options.put("driver", "org.postgresql.Driver");

        SparkSession spark = SparkSession.builder().appName("db-example").getOrCreate();

        SQLContext sqlContext = new SQLContext(spark);
        Dataset<Row> ds = sqlContext.read().format("jdbc").options(options).load();
        
        MensajeLogger.mostrarMensaje("Ejemplos usando spark");
        runSimpleExamples(ds);
        
        MensajeLogger.mostrarMensaje("Ejemplos haciendo queries a postgres");
        runSQLExamples(spark,ds);
    }
    
    public static void runSimpleExamples(Dataset<Row> ds){
    	System.out.println("Imprimimos todas las alertas");
        ds.show();
        
        System.out.println("Imprimimos el esquema de la tabla");
        ds.printSchema();
        
        System.out.println("Imprimimos el country_code de las alertas");
        ds.select("country_code").show();
        
        System.out.println("Imprimimos el country_code y las ventas aumentadas en 10");
        ds.select(col("country_code"), col("accumulated_sales").plus(10)).show();
        
        System.out.println("Imprimimos las alertas de ventas grandes");
        ds.filter(col("accumulated_sales").gt(2000000)).show();
        
        System.out.println("Imprimimos las alertas de Esapaña");
        ds.filter(col("country_code").equalTo("ES")).show();
        
        System.out.println("Imprimimos el número de alertas de cada país");
        ds.groupBy(col("country_code")).count().show();
    }
    
    public static void runSQLExamples(SparkSession spark, Dataset<Row> ds){
        ds.createOrReplaceTempView("alerts");
        
        System.out.println("Imprimimos todas las alertas");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM alerts");
        sqlDF.show();
    }
}
