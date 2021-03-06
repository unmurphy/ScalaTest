import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created on 2016/10/24
  * 主程序--接收到处理订单消息
  *
  * @author annpeter.it@gmail.com
  */
class End_1 extends Serializable {

    def getValue(line: String): Any = {
        val hour = line.substring(0, 2).toLong
        val minute = line.substring(3, 5).toLong
        val second = line.substring(6, 8).toLong
        val millisecond = line.substring(9, 12).toLong
        (((hour * 60) + minute) * 60 + second) * 1000 + millisecond
    }

    def getUpLogLine(line: String): String = {
        val orderNo = line.substring(48).trim()
        orderNo
    }

    def getDownLogLine(line: String): String = {
        null
    }

    def getSparkRDD(context: SparkContext): RDD[(String, Any)] = {

        val file = "file:///Users//yangzhixiang//Documents//Murphy_Code//ScalaTest//src//main//resources//info.log"
        val keyword = "INFO  c.w.m.LaunchMain"

        context.textFile(file).filter(_.contains(keyword)).map(line => {
            val value = getValue(line)
            val upLogLine = getUpLogLine(line)
            println("info" + upLogLine)
            (upLogLine, value)
        })
    }
}
