package com.wdk.uw

import org.apache.spark.sql.SparkSession

/**
  * @Description:
  *              用户行为分析统计每个用户，
  *              历史观看列表（评分低的过滤掉，并且按照评分做排序，取top5）
  * @Author:wang_dk
  * @Date:2019 /12/14 0014 14:06
  * @Version: v1.0
  **/
object UserWatchList {
    def main(args: Array[String]): Unit = {
        //构建spark环境
        val spark = SparkSession.builder()
                .appName("UserWatchList")
                .enableHiveSupport()
                .getOrCreate();

        var input_path = spark.sparkContext.textFile("/spark/20191207/train_new.data")

        var output_path = "/spark/20191207/userWatchList_output"

        //step1  先把评分小于2.0的过滤掉
        var rdd = input_path.filter(line=>{
            line.split("\t").length==3 && line.split("\t")(2).toDouble>2.0
        }).map(line =>{
            //step 2转成tuple 格式 (user_id,(item_id,score))
            var ss = line.split("\t");
            var user_id = ss(0);
            var item_id = ss(1);
            var score = ss(2);
            (user_id,(item_id,score))
        }).groupByKey()//step3 根据user_id 分组
            .map(line=>{
            //step4 根据分数排序
            var user_id = line._1;

            var watchList = line._2;

            var tmp_arr = watchList.toArray.sortWith(_._2>_._2);

            //step5 取top5
            var top5 = tmp_arr.length;
            if(top5>5){
                top5 = 5;
            }

            //step6 格式化输出
            var strBuf = new StringBuilder();
            for (i<- 0.until(top5)){
                strBuf.append(tmp_arr(i)._1).append(":").append(tmp_arr(i)._2).append(" ")
            }

            (user_id.toInt,strBuf.toString())
        }).sortBy(_._1) //根据userID排序

        rdd.saveAsTextFile(output_path);
    }

}
