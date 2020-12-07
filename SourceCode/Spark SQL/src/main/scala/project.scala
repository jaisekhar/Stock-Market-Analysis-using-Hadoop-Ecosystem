import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object project {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("ICP 10")
      .config("spark.master", "local")
      .getOrCreate()
    val SQLContext = spark.sqlContext

    // 1.1 Import the dataset and create data frames directly on import.
    val df = SQLContext.read.json("C:/Users/nikhi/Desktop/tweets1.json")
    val df1 = SQLContext.read.csv("C:/Users/nikhi/Desktop/out_data.csv")
    df.createOrReplaceTempView("dataset")
    df1.createOrReplaceTempView("dataset1")


    val t1=System.nanoTime()
    // 10. User name and total number of tweets  in descending order
    val q10=SQLContext.sql("select user.name, count(*) as count from dataset group by user.name order by count desc limit 10")
    q10.show()
    print("Runtime: " + (System.nanoTime()-t1)/1000/1000 + " ms")


/*    println("Aggregate Function: AVG")
    spark.sql("desc dataset").show()
 */
  }
}
