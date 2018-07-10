package com.autentia.tutoriales.ml;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;




public class SparkElasticsearchConnection {
	public static Dataset<Row> getData(String ip, String port) {
		SparkConf conf = new SparkConf().setAppName("db-example")
			.set("es.nodes", ip)
			.set("es.port", port)
			.set("es.nodes.wan.only", "true")		//Esto es porque está en docker
			.set("es.resource", "ventas-*");
				
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        SQLContext sql = new SQLContext(spark);
        
        Dataset<Row> solds = JavaEsSparkSQL.esDF(sql);
        
        return solds;
	}
	
	public static Dataset<Row> getDataFromQuery(String ip, String port, String query) {
		SparkConf conf = new SparkConf().setAppName("db-example")
			.set("es.nodes", ip)
			.set("es.port", port)
			.set("es.nodes.wan.only", "true")		//Esto es porque está en docker
			.set("es.resource", "ventas-*")
			.set("es.query", query);
		
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        SQLContext sql = new SQLContext(spark);
	        
        return JavaEsSparkSQL.esDF(sql);
	}
}
