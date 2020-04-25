import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

//to display in the terminal

//namesDF.show()

//To write data into json file

//namesDF.coalesce(1).write.json("F:\\PBPhase2\\q4")
//namesDF.coalesce(1).write.format('csv').save('F:\\PBPhase2\\q5.csv', header = 'true')
//namesDF.write.format("csv").save("F:\\PBPhase2\\pythonvisualization\\querysuccess.csv")


object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()
    System.setProperty("hadoop.home.dir", "C:\\spark-2.4.5-bin-hadoop2.7\\")
    import spark.implicits._


    //val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")


    //val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)

    val employeeDF=spark.read.json("F:\\PBPhase2\\tweetsdata.txt")
    //query1
    employeeDF.createOrReplaceTempView("parquetFile")
    val place=spark.sql("SELECT distinct place.country, count(*) as count FROM parquetFile where place.country is not null " + "GROUP BY place.country ORDER BY count DESC")
    //namesDF.map(attributes => "Name:" + attributes(0)).show()
     place.coalesce(1).write.csv("F:\\PBPhase2\\q1")

    //query2 which location is having max followers
    val location=spark.sql("select distinct user.location, max(user.followers_count) AS followers_count from parquetFile group by user.location order by followers_count  desc LIMIT 10")
    location.coalesce(1).write.csv("F:\\PBPhase2\\q2")

    //query3  user having highest no of friends
    val highest_friends= spark.sql("select user.name,user.friends_count from parquetFile order by user.friends_count desc LIMIT 10")
    highest_friends.coalesce(1).write.csv("F:\\PBPhase2\\q3")

    //query 4  Top 10 Users Who are actively liking tweets.
    val active_liking= spark.sql("select user.name,user.favourites_count from parquetFile order by user.favourites_count desc limit 15")
    active_liking.coalesce(1).write.csv("F:\\PBPhase2\\q4")
    //query 5 More tweets coming from locations
    val more_tweets= spark.sql("select user.location as location,count(*) as nooftweets from parquetFile where user.location is not null group by user.location order by count(1) desc limit 10")
    more_tweets.coalesce(1).write.csv("F:\\PBPhase2\\q5")
    //query 6 tweets having high num of likes
    val high_likes= spark.sql("select user.screen_name,user.favourites_count from parquetFile order by favourites_count desc LIMIT 10")
    high_likes.coalesce(1).write.csv("F:\\PBPhase2\\q6")
    //query 7 Account verification
    val Accountverification=spark.sql("SELECT distinct id,CASE WHEN user.verified like '%true%' THEN 'VERIFIED ACCOUNT' WHEN user.verified like '%false%' THEN 'NOT VERIFIED ACCOUNT' END AS Verify FROM parquetFile")
    Accountverification.createOrReplaceTempView("acctVerify")
    val verifieddata=spark.sql("SELECT Verify,count(Verify) as count from acctVerify where id is NOT NULL group by Verify order by count DESC")
    verifieddata.coalesce(1).write.csv("F:\\PBPhase2\\q7")


    //query 8 users created in a month
    val users_created=spark.sql("SELECT substring(created_at, 1,3) as month, count(1) users from parquetFile group by month")
    users_created.coalesce(1).write.csv("F:\\PBPhase2\\q8")
    //query 9 top 10 lang

    //val top_lang=spark.sql("SELECT distinct lang from parquetFile group by lang order by lang desc limit 10")
    //top_lang.coalesce(1).write.csv("F:\\PBPhase2\\q9")

    //query 9 corona tweets
    val coronatext=spark.sql("SELECT count(text) as count from parquetFile where text like '%corona%'")
    val extendedtweet=spark.sql("SELECT count(extended_tweet.full_text) as count from parquetFile where extended_tweet.full_text like '%corona%'")
    val corona_retweetedstatus=spark.sql("SELECT count(retweeted_status.text ) as count from parquetFile where retweeted_status.text like '%corona%'")
    val totalcoronatweets=coronatext.union(extendedtweet).union(corona_retweetedstatus)
    totalcoronatweets.coalesce(1).write.csv("F:\\PBPhase2\\q9")

    //query 10 is user protected or not
    //val user_protected= spark.sql("select user.protected from parquetFile where user.protected like '%true%'")
    //user_protected.coalesce(1).write.csv("F:\\PBPhase2\\q10")
    //query10 TOTAL TWEETS success
    val  text=spark.sql("SELECT  count(text) as count from parquetFile")
    val extended_tweet=spark.sql("SELECT count(extended_tweet.full_text) as count from parquetFile")
    val retweeted_status=spark.sql("SELECT count(retweeted_status.text ) as count from parquetFile")
    val total_tweets=text.union(extended_tweet).union(retweeted_status)
    total_tweets.coalesce(1).write.csv("F:\\PBPhase2\\q10")




  }

}
