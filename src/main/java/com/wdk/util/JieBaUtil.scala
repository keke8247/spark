package com.wdk.util

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2019/12/10 10:26
  * @Since version 1.0.0
  */
class JieBaUtil {

    def jieba_split(df:DataFrame,columnNme:String): DataFrame ={
        var sparkSession = df.sparkSession;

        val segmenter = new JiebaSegmenter();

        var broadcastSeg = sparkSession.sparkContext.broadcast(segmenter);

        var exeSegmenter = broadcastSeg.value;

        //自定义函数 分词
        val jiebaUdf = udf{(sentence:String)=>{
                exeSegmenter.process(sentence.toString,SegMode.INDEX)
                    .toArray.map(_.asInstanceOf[SegToken].word)
                    .filter(_.length>1).mkString("/")
            }
        }

        val wordContMethod = udf{(sentence:String)=>{
                exeSegmenter.process(sentence.toString,SegMode.INDEX)
                    .toArray.map(_.asInstanceOf[SegToken].word)
                    .filter(_.length>1)
                    .map(word=>(word,1))
                    .groupBy(_._1)
                    .mapValues(wordList=>wordList.map(_._2).sum)
                    .toList
                    .sortWith((pre,suf)=>pre._2>suf._2)
            }
        }

        df.withColumn("seg",jiebaUdf(col(columnNme)))
        df.withColumn("wordCont",wordContMethod(col(columnNme)))
    }


}
