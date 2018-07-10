package com.autentia.tutoriales.ml.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.autentia.tutoriales.ml.log.MensajeLogger;

public class SparkContext {
	
	public static void inicializar() {
		MensajeLogger.mostrarMensaje("Inicializar Spark");
		
        SparkConf conf = new SparkConf().setAppName("db-example").setMaster("local[*]");
        
		MensajeLogger.mostrarMensaje("Creamos JavaSparkContext (No se utiliza pero de momento hay que hacerlo)");
		
        @SuppressWarnings({ "resource", "unused" })
		JavaSparkContext sc = new JavaSparkContext(conf);
	}
	
}
