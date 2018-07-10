package com.autentia.tutoriales.ml.Examples;

import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.ChiSquareTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import com.autentia.tutoriales.ml.context.SparkContext;
import com.autentia.tutoriales.ml.log.MensajeLogger;

public class HypothesisTestingExample {
	public static void main(String[] args) {
        MensajeLogger.mostrarMensaje("Apagar los logs no importantes de spark");
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        
        //Inicializar Spark
        SparkContext.inicializar();
		SparkSession spark = SparkSession.builder().appName("JavaCorrelationExample").getOrCreate();

		List<Row> data = Arrays.asList(
	      RowFactory.create(1.0, Vectors.dense(0.5, 10.0, 7.0)),
	      RowFactory.create(1.0, Vectors.dense(1.5, 20.0, 7.0)),
	      RowFactory.create(1.0, Vectors.dense(1.5, 30.0, 7.0)),
	      RowFactory.create(0.0, Vectors.dense(3.5, 30.0, 7.1)),
	      RowFactory.create(0.0, Vectors.dense(4.5, 40.0, 7.1)),
	      RowFactory.create(1.0, Vectors.dense(1.5, 40.0, 7.0)),
	      RowFactory.create(1.0, Vectors.dense(1.5, 40.0, 7.0)),
	      RowFactory.create(1.0, Vectors.dense(1.5, 40.0, 7.0))
	    );

	    StructType schema = new StructType(new StructField[]{
	      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
	      new StructField("features", new VectorUDT(), false, Metadata.empty()),
	    });

	    Dataset<Row> df = spark.createDataFrame(data, schema);
	    Row r = ChiSquareTest.test(df, "features", "label").head();
	    System.out.println();
	    System.out.println("pValues: " + r.get(0).toString());
	    System.out.println("degreesOfFreedom: " + r.getList(1).toString());
	    System.out.println("statistics: " + r.get(2).toString());

	    spark.stop();
	}
}
