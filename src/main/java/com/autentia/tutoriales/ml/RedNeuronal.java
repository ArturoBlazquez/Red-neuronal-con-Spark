package com.autentia.tutoriales.ml;

import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RedNeuronal {
	
	private static final int DEFAULT_MAX_ITER = 300;
	private static final int DEFAULT_BLOCK_SIZE = 128;
	private static final long DEFAULT_SEED = System.currentTimeMillis();
	private static final int DEFAULT_PORCENTAJE_TEST = 20;
	private static final int[] DEFAULT_NEURONAS_INTERMEDIAS = new int[] {10,6,3};
	
	private int neuronasEntrada;
	private int neuronasSalida;
	private int[] neuronasIntermedias;
	private int porcentajeTest;
	private long seed;
	private int blockSize;
	private int maxIter;
	
	private int[] neuronas;
	
	private PipelineModel model;
	
	
	public RedNeuronal(int neuronasEntrada, int neuronasSalida) {
		this(neuronasEntrada, neuronasSalida, DEFAULT_NEURONAS_INTERMEDIAS);
	}
	
	public RedNeuronal(int neuronasEntrada, int neuronasSalida, int[] neuronasIntermedias) {
		this(neuronasEntrada, neuronasSalida, neuronasIntermedias, DEFAULT_PORCENTAJE_TEST);
	}
	
	public RedNeuronal(int neuronasEntrada, int neuronasSalida, int[] neuronasIntermedias, int porcentajeTest) {
		this(neuronasEntrada, neuronasSalida, neuronasIntermedias, porcentajeTest, DEFAULT_SEED);
	}
	
	public RedNeuronal(int neuronasEntrada, int neuronasSalida, int[] neuronasIntermedias, int porcentajeTest, long seed) {
		this(neuronasEntrada, neuronasSalida, neuronasIntermedias, porcentajeTest, seed, DEFAULT_BLOCK_SIZE);
	}
	
	public RedNeuronal(int neuronasEntrada, int neuronasSalida, int[] neuronasIntermedias, int porcentajeTest, long seed, int blockSize) {
		this(neuronasEntrada, neuronasSalida, neuronasIntermedias, porcentajeTest, seed, blockSize, DEFAULT_MAX_ITER);
	}
	
	public RedNeuronal(int neuronasEntrada, int neuronasSalida, int[] neuronasIntermedias, int porcentajeTest, long seed, int blockSize, int maxIter) {
		this.neuronasEntrada = neuronasEntrada;
		this.neuronasSalida = neuronasSalida;
		this.neuronasIntermedias = neuronasIntermedias;
		this.porcentajeTest = porcentajeTest;
		this.seed = seed;
		this.blockSize = blockSize;
		this.maxIter = maxIter;
		
		this.setNeuronas();
	}
	
	

	public void entrenar(Dataset<Row> datos, String columnaDeClasificacion, String... columnasDeDatos) {
		System.out.println("Dividimos los datos en " + (100-porcentajeTest) + "% entrenamiento y " + porcentajeTest + "% test");
		Dataset<Row>[] splits = datos.randomSplit(new double[]{(100-porcentajeTest)/100.0, porcentajeTest/100.0}, seed);
		Dataset<Row> train = splits[0];
		Dataset<Row> test = splits[1];

		
		System.out.println("Tenemos una red Neuronal con " + neuronasEntrada + " neuronas de entrada y " + neuronasSalida + " neuronas de salida");
		System.out.println("Las capas intermedias son: " + Arrays.toString(neuronasIntermedias));
		
		
		//Empieza la pipeline
		VectorAssembler featureExtractor = new VectorAssembler()	//Pasamos todos los datos a procesar a la columna features
				.setInputCols(columnasDeDatos)
				.setOutputCol("features");
				
		StringIndexerModel labelIndexer = new StringIndexer()		//Indexamos las clasificaciones en la columna label
				  .setInputCol(columnaDeClasificacion)
				  .setOutputCol("label").fit(datos);
		
	  	MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier()	//Red neuronal. Intenta a partir de las features deducir el label
		  .setLayers(neuronas)
		  .setBlockSize(blockSize)
		  .setSeed(seed)
		  .setMaxIter(maxIter);
	  	
	  	IndexToString labelConverter = new IndexToString()		//Pasamos las predicciones a datos legibles
	  		  .setInputCol("prediction")
	  		  .setOutputCol("predictedLabel")
	  		  .setLabels(labelIndexer.labels());
		//Acaba la pipeline
		
	  	Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {featureExtractor, labelIndexer, trainer, labelConverter});
	  	
	  	//Comprobación de errores
	  	if(labelIndexer.labels().length > neuronasSalida) {
	  		System.out.println("\n\nHay " + labelIndexer.labels().length + " clasificaciones: " + Arrays.toString(labelIndexer.labels()) + " y sin embargo hay sólo " + neuronasSalida + " neuronas de salida");
	  		System.out.println("Tienes que especificar más neuronas de salida");
	  		return;
	  	}

		// Entrenamos el modelo
		model = pipeline.fit(train);

		// Vemos la eficacia del modelo
		Dataset<Row> result = model.transform(test);
		Dataset<Row> predictionAndLabels = result.select("prediction", "label");
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
		  .setMetricName("accuracy");

		System.out.println("\n\nPorcentaje de error del entrenamiento = " + new DecimalFormat("#.##").format((1.0 - evaluator.evaluate(predictionAndLabels))*100) + "%");
    }
	
	public Dataset<Row> clasificar(Dataset<Row> datos) {
		return model.transform(datos);
	}
	
	
	
	private void setNeuronas() {
		neuronas = new int[neuronasIntermedias.length+2];
		
		neuronas[0]=neuronasEntrada;
		for (int i=0;i<neuronasIntermedias.length;i++) {
			neuronas[i+1]=neuronasIntermedias[i];
		}
		neuronas[neuronasIntermedias.length+1]=neuronasSalida;
	}
}