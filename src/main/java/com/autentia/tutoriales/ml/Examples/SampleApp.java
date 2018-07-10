package com.autentia.tutoriales.ml.Examples;

import static org.apache.spark.sql.functions.col;

import java.util.Calendar;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.autentia.tutoriales.ml.SparkElasticsearchConnection;
import com.autentia.tutoriales.ml.SparkPostgresConnection;
import com.autentia.tutoriales.ml.context.SparkContext;
import com.autentia.tutoriales.ml.log.MensajeLogger;

import static org.apache.spark.sql.functions.*;
import java.sql.Date;

public class SampleApp {
	
	private static Boolean cargarVentas = true;
	private static Boolean cargarTodasLasVentas = false;
	private static Boolean cargarAlertas = true;
	private static Boolean cargarHistorico = true;
	private static Boolean lanzarQueriesVentas = true;
	private static Boolean lanzarQueriesAlertas = true;
	private static Boolean lanzarQueriesHistorico = true;
	
	public static void main(String[] args) {
		System.out.println("Configuraciónn inicial: \n");
        System.out.println("Cargar ventas de BBDD de elastic: " + cargarVentas);
        System.out.println("Cargar todas las ventas: " + cargarTodasLasVentas);
        System.out.println("Cargar alertas de BBDD de " + cargarAlertas);
        System.out.println("Cargar historico de BBDD de postgres: " + cargarHistorico);
        System.out.println("Lanzar queries de ventas: " + lanzarQueriesVentas);
        System.out.println("Lanzar queries de alertas: " + lanzarQueriesAlertas);
        System.out.println("Lanzar queries de histórico: " + lanzarQueriesHistorico);
		
		
		MensajeLogger.mostrarMensaje("Apagar los logs no importantes de spark");
    	Logger.getLogger("org").setLevel(Level.WARN) ;
        Logger.getLogger("akka").setLevel(Level.WARN) ;
        
        SparkContext.inicializar();
        
        Dataset<Row> ventas=null, alertas=null, historico=null;
        
        if(cargarVentas) ventas = cargarVentas();
        if(cargarAlertas) alertas = cargarAlertas();
        if(cargarHistorico) historico = cargarHistorico();
        
        if(lanzarQueriesVentas) queriesVentas(ventas);
        if(lanzarQueriesAlertas) queriesAlertas(alertas);
        if(lanzarQueriesHistorico) queriesHistorico(historico);
        
    }



	private static Dataset<Row> cargarVentas() {
		String query="{" + 
        		"    \"range\" : {" + 
        		"        \"ticket.issueDate\" : {" + 
        		"        	\"gte\":\"now-1M\"," + 
        		"        	\"lt\":\"now\"" + 
        		"        }" + 
        		"    }" + 
        		"  }";

        
        Dataset<Row> ventas;
        if(cargarTodasLasVentas) { //NO se pueden cargar las dos a la vez. SOLO UNA SESION DE SPARK
            MensajeLogger.mostrarMensaje("ELASTICSEARCH: Carga de BBDD entera");
        	ventas = SparkElasticsearchConnection.getData("localhost","9200");
        }else{
            MensajeLogger.mostrarMensaje("ELASTICSEARCH: Carga de BBDD filtrada por query");
        	ventas = SparkElasticsearchConnection.getDataFromQuery("localhost","9200", query);
        }
		return ventas;
	}

	private static Dataset<Row> cargarAlertas() {
        MensajeLogger.mostrarMensaje("POSTGRES: Carga de alertas");
		return SparkPostgresConnection.getData("localhost", "postgres", "postgres", "example", "beagle.alerts");
	}

	private static Dataset<Row> cargarHistorico() {
        MensajeLogger.mostrarMensaje("POSTGRES: Carga de historico");
		return SparkPostgresConnection.getData("localhost", "postgres", "postgres", "example", "beagle.historical_sales");
	}

	private static void queriesVentas(Dataset<Row> ventas) {
		MensajeLogger.mostrarMensaje("Empiezan las consultas de ventas (BBDD Elasticsearch)");

        //mostrarMensaje("Propiedades de la tabla");
        //ventas.describe().show();
        
        //mostrarMensaje("Filtro por agencia y operation code");
        //ventas.filter(col("ticket.agentNumericCode").equalTo("3230119")).filter(col("ticket.operationCode").equalTo("SG")).show();
        
        //Ver lo que gana cada agencia
        Calendar cal = Calendar.getInstance();
        Date now = new Date(cal.getTime().getTime());
        cal.add(Calendar.MONTH, -1);
        Date lastMonth = new Date(cal.getTime().getTime());

        MensajeLogger.mostrarMensaje("Suma de cashAmount y ticketAmount por agentNumericCode haciendo el filtro de fechas con Spark");
        ventas.filter(col("ticket.issueDate").lt(now)).filter(col("ticket.issueDate").geq(lastMonth)).groupBy(col("ticket.agentNumericCode")).sum("ticket.cashAmount", "ticket.ticketAmount").show();
        
        MensajeLogger.mostrarMensaje("Suma de cshAmount y ticketAmount por agentNumericCode");
        ventas.groupBy(col("ticket.agentNumericCode")).agg(sum("ticket.cashAmount"), sum("ticket.ticketAmount")).show();
        // ventas.groupBy(col("ticket.agentNumericCode")).sum("ticket.cashAmount", "ticket.ticketAmount").show(); //Misma query. Tardan lo mismo
	}

	private static void queriesAlertas(Dataset<Row> alerts) {
		MensajeLogger.mostrarMensaje("Empiezan las consultas de alertas (BBDD Postgres)");
        
        MensajeLogger.mostrarMensaje("Numero de alertas por pais");
        alerts.groupBy(col("country_code")).count().show();

        MensajeLogger.mostrarMensaje("Numero de alertas por pais con query directa a BBDD SQL");
        SparkPostgresConnection.getDataFromQuery(alerts, "alerts", "select country_code, count(*) from alerts group by country_code").show();
	}

	private static void queriesHistorico(Dataset<Row> historical) {
		MensajeLogger.mostrarMensaje("Empiezan las consultas de historico (BBDD Postgres)");
		
        MensajeLogger.mostrarMensaje("Muestra las alertas de cada agencia, en cada pais, por mes");
        historical.groupBy(col("agent_code"),col("country_code"),col("month")).count().select(col("agent_code"),col("count")).show();

        MensajeLogger.mostrarMensaje("Muestra el historico de ventas de una agencia por pais y mes con query directa a BBDD SQL");
        SparkPostgresConnection.getDataFromQuery(historical, "historical_sales", "select agent_code, count(*) from historical_sales group by agent_code, country_code, \"month\"").show();
	}
}
