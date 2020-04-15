
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SchemaDataFrame {



    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrame-FromRowsAndSchema")
                .master("local[4]")
                .getOrCreate();

        List<Row> customerRows = Arrays.asList(
                RowFactory.create(1, "Widget Co", 120000.00, 0.00, "AZ"),
                RowFactory.create(null, null, null, null, null),
                RowFactory.create(3, null, 410500.00, 200.00, "CA"),
                RowFactory.create(4, "Widgets R Us", 410500.00, 0.0, null),
                RowFactory.create(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
        );

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("sales", DataTypes.DoubleType, true),
                DataTypes.createStructField("discount", DataTypes.DoubleType, true),
                DataTypes.createStructField("state", DataTypes.StringType, true)
        );
        StructType customerSchema = DataTypes.createStructType(fields);

        Dataset<Row> customerDF =
                spark.createDataFrame(customerRows, customerSchema);


        System.out.println("*** the schema created");
        customerDF.printSchema();

        System.out.println("*** the data");
        customerDF.show();

        System.out.println("*** just the rows from CA");
        customerDF.filter(col("state").equalTo("CA")).show();

        Dataset<Row> filtered = customerDF.filter(row -> !row.anyNull());


      //  filtered.show();

        ArrayList<String> list = new ArrayList<String>();


        list.addAll(Arrays.asList("id","sales"));

        ArrayList<String> df_columns = new ArrayList<>(Arrays.asList(customerDF.columns()));

       customerDF.foreach((ForeachFunction<Row>) row -> validate(list,  row,df_columns));
        customerDF.select("id").show();

System.out.println("before wit column");



        Dataset<Row> withId=customerDF.withColumn("SeqId",concat(col("id"),lit('_'),col("name"),lit('_'),monotonically_increasing_id()));
        //monotonically_increasing_id()

        //sf.concat(sf.col('colname1'),sf.lit('_'), sf.col('colname2')

               withId.show();
        System.out.println("after wit column");
        //String[] cv=customerDF.columns();


        //System.out.println("value"+cv[0]);
        spark.stop();


    }


    public static  void validate (ArrayList<String> cl,Row row,ArrayList<String> cv)
        {
           // Dataset<Row> ds=spark.emptyDataFrame();
            SparkSession spark = SparkSession
                    .builder()
                    .appName("DataFrame-FromRowsAndSchema")
                    .master("local[4]")
                    .getOrCreate();
            //Dataset<Row> ds=spark.emptyDataFrame();
            Object scolumns[]= cl.toArray();
            Object Dcolumns[] =cv.toArray();
           // System.out.println("rows"+row);



System.out.println(" "+ row.mkString().equals(null));


ArrayList<Integer> al=new ArrayList<Integer>();

            for(int i=0;i<scolumns.length;i++)
            {
                for(int j=0;j<Dcolumns.length;j++)
                {

                   if(Dcolumns[j].toString().contains(scolumns[i].toString()))
                   {
                       //System.out.println( "index "+i);
                       al.add(i);
                   }
                }
            }
System.out.println(" list"+al);







/*                 for(int i=0;i<obj.length;i++) {

                      ds.select("*").filter(col(obj[i].toString()).isNotNull()).show();
                      // df.filter(col("col1").isNotNull && col("col2").isNotNull).show
                  }
*/



           //Dataset<Row> ds=spark.emptyDataFrame();

       //System.out.println(obj[0]);


        }


      //  JavaRDD<Row>  fdata= customerDF.javaRDD();


        //map(x->col(x).isNotNull).reduce(_ && _);




}




