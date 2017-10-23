import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by yangzhixiang on 2017/6/6.
  */
object Launch {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("log-time-analyse").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = new End_1().getSparkRDD(sc)
    val rdd2 = new Dispatcher_1().getSparkRDD(sc)
    System.setProperty("hadoop.home.dir", "/Users/yangzhixiang/Documents/Tools/hadoop-2.7.3")
    val list = new ListBuffer[(Long, Long, String)]()
    rdd1.leftOuterJoin(rdd2).collect().foreach(item => {
      var time = item._2._1.asInstanceOf[Long] - item._2._2.get.asInstanceOf[Long]
      //            if(time>90000){
      //                time = time-2*60*1000
      //            }
      //            else{
      //                time = time-1*60*1000
      //            }
      list.append((item._2._2.get.asInstanceOf[Long], time, item._1))
    })


    val sorted = list.sortWith((item1, item2) => item1._1 < item2._1)
    val writer = new PrintWriter(new File("out-1.csv"))
    sorted.toList.foreach(item => {
      writer.write(item._1 + ", " + item._2 + ", " + item._3 + "\n")
    })
    writer.close()
    val sortedList = sorted.map(item => item._2).sortWith((item1, item2) => item1 < item2).toList
    val sortedListSize = sortedList.size
    println("0.5---" + sortedList((sortedListSize * 0.5).toInt))
    println("0.66---" + sortedList((sortedListSize * 0.66).toInt))
    println("0.75---" + sortedList((sortedListSize * 0.75).toInt))
    println("0.8---" + sortedList((sortedListSize * 0.8).toInt))
    println("0.9---" + sortedList((sortedListSize * 0.9).toInt))
    println("0.95---" + sortedList((sortedListSize * 0.95).toInt))
    println("0.98---" + sortedList((sortedListSize * 0.98).toInt))
    println("0.99---" + sortedList((sortedListSize * 0.99).toInt))
    println("MAX---" + sortedList.max)
    println("MIN---" + sortedList.min)
    println("AVG---" + sortedList.sum / sortedListSize)


    val list2 = sorted.groupBy(item => item._1 / 500).map(item => (item._1, item._2.size))
    val writer1 = new PrintWriter(new File("out-2.csv"))
    val sorted1 = list2.toList.sortWith((item1, item2) => item1._1 < item2._1)
    sorted1.foreach(item => {
      writer1.write(item._1 + "-" + item._2 + "\n")
    })
    writer1.close()
  }
}
