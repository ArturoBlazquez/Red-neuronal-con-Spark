package com.autentia.tutoriales.ml;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import com.autentia.tutoriales.ml.context.SparkContext;
import com.autentia.tutoriales.ml.log.MensajeLogger;

import scala.collection.JavaConverters;
import java.util.List;

import static org.apache.spark.sql.functions.*;


public class Clientes {
	
    private static final String DEFAULT_DATE = "2018-06-12T08:50:01";
	
	public static void main(String[] args) {
        MensajeLogger.mostrarMensaje("Apagar los logs no importantes de spark");
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        MensajeLogger.mostrarMensaje("Aquí podemos estar enmascarando errores");
        Logger.getLogger("org.apache.spark.sql.execution").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark.sql.catalyst.expressions.codegen").setLevel(Level.FATAL);
        Logger.getLogger("org.apache.spark.mllib.optimization").setLevel(Level.ERROR);

        SparkContext.inicializar();
        
        String date = DEFAULT_DATE;
        
        if(args.length>0) date = args[0];
        
        String query="{" + 
        		"    \"range\" : {" + 
        		"        \"timestamp\" : {" +
        		"        	\"gte\":\"" + date+ "||-90d\"," +
        		"        	\"lt\":\"" + date + "\"" +
        		"        }" + 
        		"    }" + 
        		"  }";
        
        Dataset<Row> ventas = SparkElasticsearchConnection.getDataFromQuery("localhost","9200", query);
        Dataset<Row> historico = SparkPostgresConnection.getData("localhost", "postgres", "postgres", "example", "ventas.historico");
        Dataset<Row> configuracion = SparkPostgresConnection.getData("localhost", "postgres", "postgres", "example", "ventas.umbrales");
        
        MensajeLogger.mostrarMensaje("Agrupamos las ventas en grupos de 1h");
        ventas = ventas.withColumn("timestamp_redondeado", col("timestamp").substr(0, 13));
        Dataset<Row> ventasAgrupadas = ventas.groupBy(col("detalles.idCliente").as("id_cliente"), col("detalles.pais"), col("timestamp_redondeado")).agg(sum("detalles.cantidad").as("cantidad"));
        ventasAgrupadas.cache().show(5);
        
        MensajeLogger.mostrarMensaje("Sumamos las ventas de las anteriores 24h para el mismo cliente/pais");
        WindowSpec window24h = Window.partitionBy(col("id_cliente"), col("pais"))
        		.orderBy(col("timestamp_redondeado").cast("timestamp").cast("long"))
        		.rangeBetween(-60*60*24, 0);
        Dataset<Row> ventasAgrupadasConAnteriores = ventasAgrupadas.select(col("*"), sum("cantidad").over(window24h).alias("cantidad_ultimas_24h"));
        ventasAgrupadasConAnteriores.cache().show(5);
        
       
        
        MensajeLogger.mostrarMensaje("Cargmos los datos de entrada");
        Dataset<Row> ultimoMesDelHistorico = historico.groupBy(col("id_cliente"), col("pais")).agg(max("mes").as("mes"));
        Dataset<Row> cantidadHistorico = historico.select(col("id_cliente"),col("pais"),col("cantidad").divide(30).as("ventas_diarias"),col("mes")).join(ultimoMesDelHistorico, joinBy("id_cliente","pais","mes"));
     
        Dataset<Row> historicoYUmbrales = cantidadHistorico.join(configuracion,"pais")
            	.select(col("id_cliente"),col("pais"),col("ventas_diarias"),col("umbral_cantidad_pico"),col("umbral_cantidad_gran_pico"),col("umbral_porcentaje_pico"),col("umbral_porcentaje_gran_pico"));

        Dataset<Row> ventasHistoricoYUmbrales = ventasAgrupadasConAnteriores.join(historicoYUmbrales, joinBy("id_cliente","pais"));
        ventasHistoricoYUmbrales.cache().show(5);

        
        
        MensajeLogger.mostrarMensaje("Cargamos los datos de salida");
        Dataset<Row> combinadas = ventasHistoricoYUmbrales.select(
        		col("cantidad_ultimas_24h"),col("ventas_diarias"),col("umbral_cantidad_pico"),col("umbral_cantidad_gran_pico"),col("umbral_porcentaje_pico"),col("umbral_porcentaje_gran_pico"),
                calculateAlert("umbral_cantidad_pico", "umbral_porcentaje_pico").as("pico"),
                calculateAlert("umbral_cantidad_gran_pico", "umbral_porcentaje_gran_pico").as("gran_pico"),
                (col("cantidad_ultimas_24h").minus(col("ventas_diarias"))).divide(col("ventas_diarias")).multiply(100).as("porcentaje"),
                (col("cantidad_ultimas_24h").minus(col("ventas_diarias")).as("resta")));

        Dataset<Row> datos = combinadas.select(col("*"), when(col("gran_pico").equalTo(true), "GRAN_PICO").when(col("pico").equalTo(true), "PICO").otherwise("NO_PICOS").as("picos")).filter(col("porcentaje").isNotNull());
        datos.cache().show();
        
        
        int porcentajeTest=80;
        Dataset<Row>[] splits = datos.randomSplit(new double[]{(100-porcentajeTest)/100.0, porcentajeTest/100.0}, System.currentTimeMillis());
		Dataset<Row> train = splits[0];
		Dataset<Row> test = splits[1];
        
        MensajeLogger.mostrarMensaje("Entrenamos la red neuronal");
        String [] entradas = new String [] {"cantidad_ultimas_24h", "ventas_diarias", "umbral_cantidad_pico", "umbral_cantidad_gran_pico", "umbral_porcentaje_pico", "umbral_porcentaje_gran_pico", "resta", "porcentaje"};
        RedNeuronal redNeuronal = new RedNeuronal(entradas.length, 3, new int[]{20});
        train.cache();
        redNeuronal.entrenar(train, "picos", entradas);
        
        
        MensajeLogger.mostrarMensaje("Clasificamos nuestros datos");
        Dataset<Row> datosClasificados =  redNeuronal.clasificar(test);
        datosClasificados.cache();
        
        
        double datosAcertados = datosClasificados.filter(col("picos").equalTo(col("predictedLabel"))).count();
        double datosTotales = datosClasificados.count();
        
        
        System.out.println(datosAcertados / datosTotales*100 + "% de aciertos");
        MensajeLogger.mostrarMensaje("Número de picos y grandes picos");
        double picos = datosClasificados.filter(col("picos").equalTo("PICO")).count();
        double grandesPicos = datosClasificados.filter(col("picos").equalTo("GRAN_PICO")).count();
        double noPicos = datosClasificados.filter(col("picos").equalTo("NO_PICOS")).count();
        System.out.println(picos + " picos");
        System.out.println(grandesPicos + " grandes picos");
        System.out.println(noPicos + " no picos\n");
        
        double total = datosClasificados.count();
        System.out.println(picos / total*100 + " % picos");
        System.out.println(grandesPicos / total*100 + "% grandes picos");
        System.out.println(total + " agrupaciones por hora/agencia/pais de ventas\n");
        
        double grandesPicosBienPredecidos = datosClasificados.filter(col("predictedLabel").equalTo("GRAN_PICO")).filter(col("picos").equalTo("GRAN_PICO")).count();
        double picosBienPredecidos = datosClasificados.filter(col("predictedLabel").equalTo("PICO")).filter(col("picos").equalTo("PICO")).count();
        double noPicosBienPredecidos = datosClasificados.filter(col("predictedLabel").equalTo("NO_PICOS")).filter(col("picos").equalTo("NO_PICOS")).count();
        
        System.out.println(grandesPicosBienPredecidos/grandesPicos*100 + "% de grandes picos bien predecidos");
        System.out.println(picosBienPredecidos/picos*100 + "% de picos bien predecidos");
        System.out.println(noPicosBienPredecidos/noPicos*100  + "% de no picos bien predecidos\n");
    }

    private static Column calculateAlert(String threshold_amount, String threshold_percentage) {
        return ((col(threshold_amount)).lt(col("cantidad_ultimas_24h").minus(col("ventas_diarias"))))
        		.and((col(threshold_percentage)).lt((col("cantidad_ultimas_24h").minus(col("ventas_diarias"))).divide(col("ventas_diarias")).multiply(100)));
    }

    public static scala.collection.Seq<String> joinBy(String... strings){
		List<String> stringList = new ArrayList<String>();
		for(String s: strings) stringList.add(s);
        return JavaConverters.asScalaIteratorConverter(stringList.iterator()).asScala().toSeq();
	}
}
