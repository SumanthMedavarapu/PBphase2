import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()
    System.setProperty("hadoop.home.dir", "C:\\spark-2.4.5-bin-hadoop2.7\\")
    import spark.implicits._


    //val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")


    //val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)

    val employeeDF=spark.read.json("F:\\PBPhase2\\tweetsdata.txt")

    employeeDF.createOrReplaceTempView("parquetFile")
    val namesDF=spark.sql("SELECT distinct place.country, count(*) as count FROM parquetFile where place.country is not null " + "GROUP BY place.country ORDER BY count DESC")
    //namesDF.map(attributes => "Name:" + attributes(0)).show()
    //to display in the terminal

    namesDF.show()

    //To write data into json file

    //namesDF.coalesce(1).write.json("F:\\PBPhase2\\q4")
    namesDF.coalesce(1).write.csv("F:\\PBPhase2\\q5")
    //namesDF.coalesce(1).write.format('csv').save('F:\\PBPhase2\\q5.csv', header = 'true')
    //namesDF.write.format("csv").save("F:\\PBPhase2\\pythonvisualization\\querysuccess.csv")



  }

}
