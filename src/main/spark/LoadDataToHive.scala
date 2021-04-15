import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LoadDataToHive {
  var conf = new SparkConf().setAppName("Spark01").setMaster("local[*]")
  var sc = new SparkContext(conf)


  def main(args: Array[String]): Unit = {
    //读取源文件
    val te: RDD[String] = sc.textFile("F:\\刘子豪\\项目数据\\ods_spider_invs_zhibo_income\\zhibo_income.20210301.txt")

    val str1: Array[String] = te.take(100000)
    //    str1.foreach(line => line)
    //将str转换成RDD格式
    val rdd: RDD[String] = sc.makeRDD(str1)
    //给数据重新分区,要不他自己会分11个区
    val rdd1: RDD[String] = rdd.repartition(1)
    //输出文件
    rdd1.saveAsTextFile("F:\\刘子豪\\项目数据\\income1\\obs.json")
    /**
      *  str1.foreach(line => line)
      * 读出数据如下:
      * {"uid": "2379086311", "crawl_time": "2021-03-01 00:00:20", "nickname": "\u795e\u79d8\u5c0f\u91ce\u732b", "cat2": "", "cat1": "other", "batch": "2021-03-01 00:00:03", "platform": "YY", "fans": "53", "extras": {"dau": "47", "list_url": "https://data.3g.yy.com/mobyy/module/sing/idx/281?page=29", "desc": "\u65b0\u4e3b\u64ad\uff0c\u6c42\u7167\u987e", "thumb": "http://zonemin.bs2.yy.com/group16/M00/5d83e3b503484c4daffeb32683596995.jpg?ips_thumbnail/3/w/363/h/330/format=jpeg", "sid": "31681866"}, "online": "214", "contribution": "4877800"}
      */
  }
}
