package racha.barbara;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;


public class App1 {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Tp1 Spark SQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df1 = sparkSession.read().option("header", "true").option("inferSchema","true").csv("products.csv");
        df1.createTempView("products"); // SQL
        //df1.printSchema();
        //df1.show();

            // 1- Les produits avec prix>=15000
        //df1.where("price >= 15000").show();
        //sparkSession.sql("select * from products where price >= 15000").show();
            // 2- Les produits avec prix<10000
        //df1.where(col("price").lt(10000)).show();
        //sparkSession.sql("select * from products where price < 10000").show();
            // 3- Le prix moyen par marque
        //df1.groupBy("Name").avg("price").show();
        //sparkSession.sql("select Name, avg(Price) from products group by Name").show();
            // 4- Affichage des produits par ordre décroissant de leur prix
        //df1.select(col("name"),col("price")).orderBy(col("price").desc()).show();

    }
}
