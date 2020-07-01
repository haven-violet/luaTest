/**
  * @Author liaojincheng
  * @Date 2020/6/30 19:50
  * @Version 1.0
  * @Description
  */
class Test {
  /*
  TODO SparkStreaming消费kafka的两种方式
    receivers
      优点:
        高级api, offset不用自己管理,zk管理 编写简单,代码少
      缺点:
        有一个预写机制wal,当你消费一次数据的时候,他还要备份一个数据
        zk如果丢失offset,那么就丢失了
    director
      优点:
        手动维护offset,只需要消费一次数据,不会备份,
      缺点:
        手动管理offset,代码复杂
   */

}
