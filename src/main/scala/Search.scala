
import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule




object SimpleApp {

  case class Person(name: String, age: Int)
  case class Group(name: String, persons: Seq[Person], leader: Person)

  case class E(e: EE)
  case class EE(timestamp: String, ip:String)


  val uuid = "F9FA3ADE-C4EE-4E4C-BC68-9F215CE62E6C"

  def filterFunc0(line: String): Boolean ={

    return line.contains(uuid)

  }


  def filterFunc(line: String): Boolean = {
    /*
        val originalMap = Map("a" -> List(1,2), "b" -> List(3,4,5), "c" -> List())
        val json = JsonUtil.toJson(originalMap)
        // json: String = {"a":[1,2],"b":[3,4,5],"c":[]}
        val map = JsonUtil.toMap[Seq[Int]](json)
        // map: Map[String,Seq[Int]] = Map(a -> List(1, 2), b -> List(3, 4, 5), c -> List())

        /*
         * Unmarshalling to a specific type of Map
         */
        val mutableSymbolMap = JsonUtil.fromJson[collection.mutable.Map[Symbol,Seq[Int]]](json)
        // mutableSymbolMap: scala.collection.mutable.Map[Symbol,Seq[Int]] = Map('b -> List(3, 4, 5), 'a -> List(1, 2), 'c -> List())

        /*
         * (Un)marshalling nested case classes
         */


        val jeroen = Person("Jeroen", 26)
        val martin = Person("Martin", 54)

        val originalGroup = Group("Scala ppl", Seq(jeroen,martin), martin)
        // originalGroup: Group = Group(Scala ppl,List(Person(Jeroen,26), Person(Martin,54)),Person(Martin,54))

        val groupJson = JsonUtil.toJson(originalGroup)
        // groupJson: String = {"name":"Scala ppl","persons":[{"name":"Jeroen","age":26},{"name":"Martin","age":54}],"leader":{"name":"Martin","age":54}}

        val group = JsonUtil.fromJson[Group](groupJson)*/



    val myMap = JsonUtil.fromJson[E](line)

    val dateStr = myMap.e.timestamp
    val ipStr = myMap.e.ip

    if(dateStr.compare("2018-xx-xx xx") < 0 || dateStr.compare("2018-xx-xx xx") > 0){
      return false
    }

    if(ipStr != "xx.xx.xx.xx"){
      return false
    }

    //  println(ipStr)

    return true

    //val parsed = JSON.parse (line)


    /*val uuidMath = (parsed \ "c" \ "uuid").asOpt[String].match{
      case Some(data) => data == "B5D1C1F5-0351-4568-A012-4D1B0C82D19C"
      case None => false
    }*/

    /*val vals = sc.parallelize(line::Nil)

    val schema1 = (new StructType).add("e", (new StructType).add("timestamp", StringType).add("ip", StringType))

    val df = sqlContext.read.schema(schema1).json(vals).select("e.timestamp", "e.ip")

    if(df.count() < 1){
      return false
    }

    val dateStr = df.first().getString(0);
    val ipStr = df.first().getString(1);
 //   println(dateStr)

    if(dateStr.compare("2018-02-15 14") < 0 || dateStr.compare("2018-02-15 16") > 0){
      return false
    }

    if(ipStr != "126.225.103.217"){
      return false
    }

  //  println(ipStr)

    return true

    /*

    val dateStr = (parsed \ "e" \ "timestamp").asOpt[String] match{
      case Some(data) => data
      case None => ""
    }

    if(dateStr.isEmpty){
      return false
    }*/

    //println(dateStr.compare("2018-02-15 17"))
   // println(dateStr.compare("2018-02-15 12"))

    /*if(dateStr.compare("2018-02-15 14") > 0 || dateStr.compare("2018-02-15 16") < 0){
      return false
    }

    val ipMatch: Boolean = (parsed \ "e" \ "ip").asOpt[String] match{
      case Some(data) => data == "126.225.103.217"
      case None => false
    }

    if(ipMatch){
      return true
    }else{
      return false
    }*/

*/
  }


  def main(args: Array[String]) {



    val testStr = """{"e":{"timestamp":"2018-xx-xx xx:xx:xx,xxxxxxx","ip":"xx.xx.xx.xx","user_agent":"Mozilla\/5.0","os":"iOS 11","api":"scheme","v":"1"},"c":{"uuid":"xxxxxx-xxxx-xxxxx"}}"""
/*    filterFunc(testStr)
    return*/

    val logFile = "C:/log/*" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    var sqlcontext = new SQLContext(sc)

    val logData = sc.textFile(logFile, 2).cache()
    //val numAs = logData.filter(line => line.contains("a") && line.contains("b")).count()
    //val numBs = logData.filter(line => line.contains("b")).take(10).foreach(println)
    //val numCs = logData.filter(line => line.contains("B5D1C1F5-0351-4568-A012-4D1B0C82D19C")).collect()//.saveAsTextFile("D:/result.txt")
    val numCs = logData.filter(line => filterFunc0(line)).collect()


    val pw = new PrintWriter(new File(s"D:/${uuid}.txt"))

    for(str <- numCs){
      pw.println(str)
    }

    pw.close()

    //val str = numCs.mkString(" ")
    //println(s"Lines with a: $numAs")
    sc.stop()
  }
}