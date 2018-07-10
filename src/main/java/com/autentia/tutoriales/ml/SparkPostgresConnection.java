package com.autentia.tutoriales.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;


public class SparkPostgresConnection {
	public static Dataset<Row> getData(String ip, String database, String user, String password, String table){
		try {
		    Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
		    System.out.println("PostgreSQL JDBC Driver Not Registered!");
		    e.printStackTrace();
		}
		
        Map<String, String> options = new HashMap<String, String>();
        String url = String.format("jdbc:postgresql://%s:5432/%s?user=%s&password=%s",ip,database,user,password);
        options.put("url", url);
        options.put("dbtable", table);
        options.put("driver", "org.postgresql.Driver");

        SparkSession spark = SparkSession.builder().appName("db-example").getOrCreate();
        
        SQLContext sqlContext = new SQLContext(spark);
        
        Dataset<Row> ds = sqlContext.read().format("org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider").options(options).load();
        
        return ds;
	}
	
	public static Dataset<Row> getDataFromQuery(Dataset<Row> ds, String tableName, String query){
		SparkSession spark = SparkSession.builder().appName("db-example").getOrCreate();
		ds.createOrReplaceTempView(tableName);

        return  spark.sql(query);
	}
}