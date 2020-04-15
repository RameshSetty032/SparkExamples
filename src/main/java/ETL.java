
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
public class ETL {
    public static void main(String args[])
    {
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SparkSession spark = SparkSession.builder().appName("xxx").master("local[*]").config("spark.sql.files.ignoreCorruptFiles", "true")
            .config("dfs.client.read.shortcircuit.skip.checksum", "true")
            .config("spark.sql.caseSensitive", "true")

            .config("spark.sql.parquet.writeLegacyFormat", "true")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS").getOrCreate();

        // provide path to input text file
        ;

        // read text file to RDD
        JavaRDD<String> lines = sc.textFile( "file:///home/rohtek/sample.txt");

        // collect RDD for printing
        List<String> listCsvLines = lines.collect();
        System.out.println("========= " + listCsvLines);


           Dataset<Row> ds= spark.read().parquet("file:///home/rohtek/Downloads/izettle_identity_purchase_created_20200302_20200302000000_031.parquet");

                   ds.printSchema();

                   ds.show();





                 //  spark.sql("create table ")



    }
}








;