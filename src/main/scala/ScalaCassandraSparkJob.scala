/**
 * Created by goldv on 7/17/2015.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object ScalaCassandraSparkJob {

  def main(args: Array[String])  {
    val conf: SparkConf = new SparkConf(true).set("spark.cassandra.connection.host", "chviromgdev01")
    val sc: SparkContext = new SparkContext("spark://chviromgdev01:7077", "test", conf)

    val tick = sc.cassandraTable("ticks", "tick")

    println( tick.first() )

   // println(tick.count())
  }

}
