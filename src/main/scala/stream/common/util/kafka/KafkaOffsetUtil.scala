package stream.common.util.kafka

import org.apache.log4j.Logger


 
object KafkaOffsetUtil {
  val logger = Logger.getLogger("KafkaOffsetUtil")




  /**
    * 过程时间
    */
  class Stopwatch {
    private val start = System.currentTimeMillis()
    override def toString() = (System.currentTimeMillis() - start) + " ms"
  }
}