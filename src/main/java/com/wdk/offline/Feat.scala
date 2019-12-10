package com.wdk.offline

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2019/12/10 15:09
  * @Since version 1.0.0
  */
object Feat {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("Feature TransForm")
            .enableHiveSupport()
            .getOrCreate();

        /** product feature
          * 1. 销售量 prod_cnt
          * 2. 商品被再次购买（reordered）量 prod_sum_rod
          * 3. 统计reordered比率prod_rod_rate  avg=sum/count [1,0,1,0,1]  3/5
          * */

        val orders = spark.sql("select * from badou.orders");
        val priors = spark.sql("select * from badou.priors");

        // 每个商品的销售数量
        val prodCnt = priors.groupBy("product_id").count().withColumnRenamed("count","prod_cnt");

        val prodRodCnt = priors.selectExpr("product_id","cast(reordered as int)")
                        .groupBy("product_id")
                        .agg(
                            sum("reordered").as("prod_sum_rod"),
                            count("product_id").as("prod_cnt"),
                            avg("reordered").as("prod_rod_rate"))
        prodRodCnt.show();

        /**
          * user Feature
          * 1. 每个用户购买订单的平均day间隔  days_since orders
          * 2. 每个用户的总订单数 count  max(order_number)
          * 3. 每个用户购买的product商品去重后的集合数据  collect_set
          * 4. 用户总商品数量以及去重后的商品数量
          * 5. 每个用户平均每个订单有多少商品
          * */

        //对异常值处理 days_since_prior_order为""的情况 转换为0.0
        var ordersNew = orders.selectExpr("*","if(days_since_prior_order='',0.0,days_since_prior_order) as dspo")
            .drop("days_since_prior_order");//drop掉原来的列
        ordersNew.show();

        //每个用户购买订单的平均day间隔  days_since orders & 总订单数 count  max(order_number)
        val userGap = ordersNew.selectExpr("user_id","cast(dspo as int) as dspo")
            .groupBy("user_id")
            .agg(
                avg("dspo").as("u_avg_day_gap"),
                count("user_id").as("u_ord_cnt"))
        userGap.show()

        //orders 和priors 表join 需要进行shuffle  序列化后缓存到 内存或者磁盘(内存不够的情况再缓存的磁盘)
        val op = orders.join(priors,"order_id").persist(StorageLevel.MEMORY_AND_DISK_SER);

        import spark.implicits._

        //3. 每个用户购买的product商品去重后的集合数据  collect_set
        val up = op.selectExpr("user_id","product_id")
        val userUniOrdRecs = up.rdd.map(
                x=>(x(0).toString,x(1).toString)) //把结果集转成tuple
            .groupByKey() //根据key(uesr_id)分组,此时product_id是一个List
            .mapValues(_.toSet.mkString(",")) //把product_id的list 转成set 起到去重的作用
                .toDF("user_id","prod_uni_cnt")

        //合并 3 4 两个统计维度
        val userProRcdSize = up.rdd.map(
            x=>(x(0).toString,x(1).toString)) //把结果集转成tuple
            .groupByKey() //根据key(uesr_id)分组,此时product_id是一个List
            .mapValues(products=>{
                val productSet = products.toSet;
                (productSet.size,productSet.mkString(","),products.size) //去重后的条数,去重后的product_id列表,未去重的总条数
            }) //把product_id的list 转成set 起到去重的作用
            .toDF("user_id","tuple") //转换成DataFrame
            .selectExpr("user_id","tuple._1 as prod_uni_cnt","tuple._2 as prod_records","tuple._3 as prod_size")//把tuple分拆成不同列


        userProRcdSize.write.mode(SaveMode.Append).save(args(0)+"user_pro_records")
        userProRcdSize.show()

        // 用聚合函数的方法 统计 3,4
        val userProRcdSize1 = up.groupBy("user_id").agg(
            collect_set("product_id").as("prod_records"),
            size(collect_set("product_id")).as("prod_uni_cnt"),
            count("product_id").as("prod_size"))

        userProRcdSize1.write.mode("overwrite").saveAsTable("badou.user_pro_records");
        userProRcdSize1.show();

        //5. 每个用户平均每个订单有多少商品
        val oneOrderProds = priors.groupBy("order_id").count().withColumnRenamed("count","ord_prod_cnts");
        val usrePerOrdProdCnt = orders.join(oneOrderProds,"order_id")
                                    .groupBy("user_id")
                                    .agg(avg("ord_prod_cnts").as("u_avg_ord_prods"));
        usrePerOrdProdCnt.show()

        /** user and product Feature :cross feature交叉特征
          * 1. 统计user和对应product在多少个订单中出现（distinct order_id）
          * 2. 特定product具体在购物车（cart）中出现位置的平均位置
          * 3. 最后一个订单id
          * 4. 用户对应product在所有这个用户购买产品量中的占比rate
          * */

        //组合key
        val userXprod = op.selectExpr("concat_ws('_',user_id,product_id) as user_prod","order_number","order_id");
        //1:统计user和对应product在多少个订单中出现（distinct order_id）
        val userXprodInOrd = userXprod.groupBy("user_prod").agg(
                approx_count_distinct("order_id").as("orderId"),
                count("user_prod").as("ordNums"))
        userXprodInOrd.show()
    }

}
