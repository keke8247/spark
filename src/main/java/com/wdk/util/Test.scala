package com.wdk.util

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2019/12/10 14:54
  * @Since version 1.0.0
  */
object Test extends App{
    var str = "可通过withColumn的源码看出withColumn的功能是实现增加一列，或者替换一个已存在的列，他会先判断DataFrame里有没有这个列名，如果有的话就会替换掉原来的列，没有的话就用调用select方法增加一列，所以如果我们的需求是增加一列的话，两者实现的功能一样，且最终都是调用select方法，但是withColumn会提前做一些判断处理，所以withColumn的性能不如select好。 "

    var segmenter = new JiebaSegmenter();

    segmenter.process(str,SegMode.INDEX)
        .toArray.map(_.asInstanceOf[SegToken].word)
        .filter(_.length>1)
        .map(x=>(x,1))
        .groupBy(_._1)
        .mapValues(list=>list.map(_._2).sum).toList.sortWith((pre,suf)=>pre._2>suf._2)
        .foreach(println(_))

}
