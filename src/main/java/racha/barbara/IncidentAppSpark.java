package racha.barbara;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class IncidentAppSpark {
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .appName("IncidentAppSpark")
                //.master("local[*]")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("Id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Titre", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Description", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Service", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Date", DataTypes.TimestampType, true, Metadata.empty())
        });

        Dataset<Row> incidents = sparkSession.readStream().schema(schema).option("header","true").csv("hdfs://namenode:8020/input");

        //1- Afficher d’une manière continue le nombre d’incidents par service.
        Dataset<Row> incidentsParService = incidents
                .groupBy("service")
                .count()
                .orderBy(desc("count"));

        StreamingQuery query1 = incidentsParService.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        //2- Afficher d’une manière continue les deux année ou il a y avait plus d’incidents.
        Dataset<Row> incidentsParAnnee = incidents
                .groupBy(year(to_date(col("date"), "yyyy-MM-dd")).alias("annee")) // Extraire directement l'année
                .count()
                .orderBy(desc("count"))
                .limit(2);

        StreamingQuery query2 = incidentsParAnnee.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query1.awaitTermination();
        query2.awaitTermination();
    }
}
