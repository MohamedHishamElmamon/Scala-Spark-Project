import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.io.FileWriter
import java.io.File
import scala.io.Source
import java.nio.file.{FileSystem, Files, Path, Paths, StandardCopyOption}




object ReadCSVFile {

  case class Data(no: String, App: String, values:Int , Times: String)
  case class Segment(no: String)
  case class Rule(app: String, appname: String, min: Int, max: Int, valuee: Int)


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val dataDF = sc.textFile(s"${args(0)}/*.csv").map {
      line =>
        val col = line.split(",")
        Data(col(0), col(1), col(2).toInt, col(3))}.toDF()
dataDF.show()
    val segmentDF = sc.textFile(s"${args(1)}/*.csv").map {
      line =>
        val col = line.split(",")
        Segment(col(0))
    }.toDF()
    segmentDF.show()
    val ruleDF = sc.textFile(s"${args(2)}/*.csv").map { line =>
        val col = line.split(",")
        Rule(col(0), col(1), col(2).toInt, col(3).toInt,col(4).toInt)
    }.toDF()
    ruleDF.show()


    val fw = new FileWriter(s"${args(3)}/out.csv", true)

    val outDF = sc.textFile(s"${args(3)}/*.csv").map{
      line =>
        val col = line.split(",")
        Data(col(0), col(1), col(2).toInt, col(3))
    }.toDF()
    println(outDF.take(1).isEmpty)
    val newDF = dataDF.join( ruleDF,
     dataDF("App") === ruleDF("app") && dataDF("values") > ruleDF("valuee")
    ).select(dataDF("no"),dataDF("App"),dataDF("values"),dataDF("Times"))

    val semifinalDF= newDF.join(segmentDF,newDF("no") === segmentDF("no")).select(newDF("no"),newDF("App"),newDF("values"),newDF("Times"))
    if (outDF.take(1).isEmpty){
      val sfinal = semifinalDF.dropDuplicates("no", "App").select(semifinalDF("no"), semifinalDF("App"), semifinalDF("values"), semifinalDF("Times"))
        .coalesce(1)
        .write.format("com.databricks.spark.csv").mode(SaveMode.Append)
        .option("header", "false")
        .save(s"${args(3)}")
    }

    else {
    val newsemiDF = semifinalDF.withColumn("new",expr(semifinalDF("Times").substr(0,8).toString()))
    val newoutputDF = outDF.withColumn("new", expr(semifinalDF("Times").substr(0, 8).toString()))
      val finalDF = newsemiDF.union(newoutputDF).dropDuplicates("no", "App", "new").select(newsemiDF("no"), newsemiDF("App"), newsemiDF("values"), newsemiDF("Times"))

   finalDF
      .coalesce(1)
      .write.format("com.databricks.spark.csv").mode(SaveMode.Append)
      .option("header", "false")
      .save(s"${args(3)}") }


    for (file <- new File(s"${args(0)}/").listFiles.filter(_.isFile)){
      println(file)
      val d1 = new File(s"${file}").toPath
      val d2 = new File(s"${args(0)}/OldData").toPath

      Files.move(d1, d2, StandardCopyOption.ATOMIC_MOVE)}


    }

    }







