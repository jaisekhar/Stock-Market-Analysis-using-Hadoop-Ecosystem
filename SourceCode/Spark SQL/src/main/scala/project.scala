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

    // 1. Total no of tweets
    val q1=SQLContext.sql("select count(*) from dataset")
    q1.show()

    // 2. Tweets from different countries
    val q2 = SQLContext.sql("SELECT place.country,count(*) AS count FROM dataset GROUP BY place.country ORDER BY count DESC limit 10")
    q2.show()

    // 3. Number of tweets based on day
    val q3 = SQLContext.sql("SELECT substring(created_at,1,3) as day,count(*) as count from dataset group by substring(created_at,1,3)")
    q3.show()

    // 4. Users with more tweets
    val q4=SQLContext.sql("SELECT count(*) as count, user.name from dataset where user.name is not null group by user.name order by count desc limit 10")
    q4.show()

    // 5. Users with more followers
    val q5 = SQLContext.sql("SELECT user.screen_name, max(user.followers_count) as followers_count FROM dataset WHERE text like '%AAPL%' group by user.name order by followers_count desc limit 15");
    q5.show()

    // 6. Number of tweets based on months
    val q6 = SQLContext.sql("SELECT substring(created_at,5,3) as month, count(*) as count from dataset group by substring(created_at,5,3)");
    q6.show()

    // 7. Number of tweets based on different locations in USA
    val q7=SQLContext.sql("SELECT user.location,count(*) as count FROM dataset WHERE place.country='United States' AND user.location is not null GROUP BY user.location ORDER BY count DESC LIMIT 15");
    q7.show()

    // 8. Polarity of tweets
    val q8=SQLContext.sql("select _c5 as polarity from dataset1 limit 10")
    q8.show()

    // 9. User Screen name with positive polarity
    val q9=SQLContext.sql("select _c3 as screenname,_c5 as polarity from dataset1 where _c5>0.0")
    q9.show()

    // 10. User name and total number of tweets  in descending order
    val t1=System.nanoTime()
    val q10=SQLContext.sql("select user.name, count(*) as count from dataset group by user.name order by count desc limit 10")
    q10.show()
    print("Runtime" +(System.nanoTime()-t1)/1000/1000 + "ms")


/*    println("Aggregate Function: AVG")
    spark.sql("desc dataset").show()
 */
  }
}
