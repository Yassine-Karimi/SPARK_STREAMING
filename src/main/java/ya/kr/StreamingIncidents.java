package ya.kr;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.count;

public class StreamingIncidents {

    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

        SparkSession ss= SparkSession.builder()
                .appName("StreamingIncidents")
                .master("local[*]")
                .getOrCreate();
        StructType incidentSchema = new StructType().add("Id","integer").add("Titre","string").add("Description","string").add("Service","string").add("Date","string");

        Dataset<Row> stockData = ss.readStream()
                .option("sep",",")
                .schema(incidentSchema)
                .csv("hdfs://localhost:9000/repo1");

        Dataset<Row> resultDf = stockData.groupBy("Service").agg(count(stockData.col("Service"))).filter("Service IS NOT NULL AND Service != 'Service'");

        /*
        Dataset<Row> incidentsByYear = df.withColumn("year", df.col("date").substr(0, 4));
        Dataset<Row> incidentsCountByYear = incidentsByYear.groupBy("year").count();
        incidentsCountByYear = incidentsCountByYear.sort(incidentsCountByYear.col("count").desc());
        incidentsCountByYear.show(2);
         */
        Dataset<Row> resultDf2 = stockData.withColumn("year", stockData.col("date").substr(0,4));
        Dataset<Row> incidentsCountByYear = resultDf2.groupBy("year").count().filter("year IS NOT NULL AND year != 'Date'");
        incidentsCountByYear = incidentsCountByYear.sort(incidentsCountByYear.col("count").desc());
        incidentsCountByYear.limit(2);




        StreamingQuery query1 = resultDf.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("8000 milliseconds"))
                .start();
//        StreamingQuery query2 = incidentsCountByYear.writeStream()
//
//                .outputMode("complete")
//                .format("console")
//                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("8000 milliseconds"))
//                .start();
//        query2.awaitTermination();
        query1.awaitTermination();

    }

}
