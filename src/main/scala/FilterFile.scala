import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangzhixiang on 2017/6/6.
  */
object FilterFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("log-time-analyse").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd2 = new Dispatcher_1().getSparkRDD(sc)
//    System.setProperty("hadoop.home.dir", "/Users/yangzhixiang/Documents/Tools/hadoop-2.7.3")

    val writer = new PrintWriter(new File("out-1.csv"))
    rdd2.collect().foreach(item => {
      writer.write(item._2 + "-" + item._1 + "\n")
    })
    writer.close()
  }
}
