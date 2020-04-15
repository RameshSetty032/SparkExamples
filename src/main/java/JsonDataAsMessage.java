import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.ArrayList;

public class JsonDataAsMessage {


    public static void main(String[] args) {


        String str1 = "{\"_id\":\"123\",\"ITEM\":\"Item 1\",\"CUSTOMER\":\"Billy\",\"AMOUNT\":285.2}";
        String str2 = "{\"_id\":\"124\",\"ITEM\":\"Item 2\",\"CUSTOMER\":\"Sam\",\"AMOUNT\":245.85}";
        List<String> jsonList = new ArrayList<>();
        jsonList.add(str1);
        jsonList.add(str2);
        SparkContext sparkContext = new SparkContext(new SparkConf()
                .setAppName("myApp").setMaster("local"));
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        SQLContext sqlContext = new SQLContext(sparkContext);
        JavaRDD<String> javaRdd = javaSparkContext.parallelize(jsonList);
        Dataset<Row> data = sqlContext.read().json(javaRdd);
        data.show();
    }
}