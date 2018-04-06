package com.zyh.main
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 

object RecSysMain {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() 
    conf.setAppName("RecSys-SPARKAPP") 
        .setMaster("spark://35.201.136.180:7077")
        .set("spark.executor.memory", "12g")
        .set("spark.executor.cores", "2")
        .set("spark.driver.memory", "20g")
    val sc = new SparkContext(conf) 
    sc.addJar("/home/yuehui/workspace/spark/RecSys/bin/RecSys.jar")
    //最小支持度
    val minSupport = 0.05
    //最小置信度
    val minConfidence = 0.6
    //数据分区数
    val numPartitions = 12
    //hdfs input url 
    val inputUrl = "hdfs://35.201.136.180:9000/data/"
    //hdfs out url 
    val outputUrl = "hdfs://35.201.136.180:9000/out/main/"
    //读取数据
    val data = sc.textFile(inputUrl + "D.dat",numPartitions)
    val user = sc.textFile(inputUrl + "U.dat",numPartitions)
    
    val transactions: RDD[Array[String]] = data.map(line => line.trim.split(' '))
    transactions.cache()
    //创建一个FPGrowth的算法实列
    //设置计算的最小支持度和数据分区
    
    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartitions)
    
    val model = fpg.run(transactions)
    
    //收集频繁项集
    val fre = model.freqItemsets.map(itemset => (itemset.freq,itemset.items.mkString(" ")))
    fre.cache()
    val freq = fre.sortByKey().collect
    val resfreq = sc.makeRDD(freq.map(u => u._2))
    //val resfreq = fre.map(x => x._2)
    //resfreq.foreach(f => println("resfreq => "+f))
    resfreq.saveAsTextFile(outputUrl+"resfreq")
    
    //推荐规则[前项, (后项, 置信度)]
    val AllAssociationRules = 
      model.generateAssociationRules(minConfidence)
      .map(x => (x.antecedent.toSet,(x.consequent.mkString(" "),x.confidence)))
      
    val rulesList =AllAssociationRules
      .combineByKey(
          List(_),
          (x:List[(String, Double)], y:(String, Double)) => y :: x, 
          (x:List[(String, Double)], y:List[(String, Double)]) => x ::: y)//不知道combineByKey是否平均分配到每个分区.repartition(numPartitions)
    
    //保存关联规则与置信度
    rulesList.map(rule => (rule._1.mkString("[", ",", "]"), rule._2.mkString("[", " | ", "]")))
     .saveAsTextFile(outputUrl+"rulesList") 
     
    //置信度最高的规则，置信度相同则合并
    val bestRules = 
      rulesList.map(rule => 
        (rule._1, (rule._2).toList.reduceLeft((a, b) => if(a._2 > b._2) a else if(a._2 == b._2) (a._1 + " " + b._1, a._2) else b)))
    bestRules.cache()
    //保存关联规则中置信度最高的
    bestRules.map(rule => rule._1.mkString("[", ",", "]") + " --> " + rule._2._1.split(" ").mkString("[", ",", "]") + " : " + rule._2._2)
      .saveAsTextFile(outputUrl + "bestRules") 
    //用户购买记录Set
    val usrSet = user.map(line => (line.trim.split(" ").toSet))
    //收集，便于遍历
    val rules = bestRules.collect()
    //计算用户推荐项
    val userRecItems = usrSet.map(
        uSet => (
            rules.filter(x => x._1.subsetOf(uSet))
              .map(x => (x._2._1.split(" ").toSet, x._2._2))
              .reduce((x, y) => if(x._2 > y._2) x else if (x._2 == y._2) (x._1 ++ y._1, x._2) else y)._1, uSet
              )
            ).map(u => (u._1.filter(x => !u._2.contains(x)), u._2))
    userRecItems.cache()
    //保存格式化
    if(userRecItems.isEmpty()){
      println("userRecItems is Empty")
    }else {
      val result = userRecItems.map(item => (item._1.toList.mkString("[", ",", "]") +" for user : " + item._2.toList.mkString("[", ",", "]")))
      result.cache()
      result.saveAsTextFile(outputUrl+"userRecItems")
    }
    
    println("end of RecSys")
  }
}