package racha.barbara;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

public class App2 {
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Spark Streaming")
                //.master("local[*]")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Quantity", DataTypes.IntegerType, true, Metadata.empty()),
        });

        Dataset<Row> inputTable = sparkSession.readStream().schema(schema).option("header","true").csv("hdfs://namenode:8020/input");
        Dataset<Row> outputTable = inputTable.groupBy("name").count();
        StreamingQuery streamingQuery = outputTable.writeStream()
                .outputMode("complete") // avant c'était "append" puisqu'on a fait pas d'agrégations
                .format("console")
                .trigger(Trigger.ProcessingTime(5000)) //Batch processing time
                .start();
        streamingQuery.awaitTermination();
    }
}
