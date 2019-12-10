package com.wdk.offline

import com.huaban.analysis.jieba.JiebaSegmenter
import com.wdk.util.JieBaUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2019/12/10 10:20
  * @Since version 1.0.0
  */
object JieBa {
    def main(args: Array[String]): Unit = {
        //定义结巴分词类的序列化
        var conf = new SparkConf().registerKryoClasses(Array(classOf[JiebaSegmenter]));

        //初始化spark环境
        var spark = SparkSession.builder()
                            .appName("结巴分词")
                            .master("yarn-cluster")//可以再运行脚本文件中指定
                            .config(conf)
                            .enableHiveSupport()
                            .getOrCreate();

        //使用DataFrame方式操作hive数据

        val df = spark.sql("select * from badou.news_noseg");

        //进行结巴分词
        var df_seg = new JieBaUtil().jieba_split(df,"sentence");
        df_seg.show(50)
    }
}
