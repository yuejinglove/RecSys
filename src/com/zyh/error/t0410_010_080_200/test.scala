package com.zyh.error.t0410_010_080_200
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 
object Test {
def main(args: Array[String]): Unit = {
    val conf = new SparkConf() 
    conf.setAppName("RecSys-SPARKAPP") 
        .setMaster("spark://master:7077")
        .set("spark.executor.memory", "15g")
        .set("spark.executor.cores", "1")
        .set("spark.driver.memory", "20g")
    val sc = new SparkContext(conf) 
    val minSupport = 0.100
    val minConfidence = 0.80
    val numPartitions = 200
    val inputUrl = "hdfs://master:9000/data/"
    val outputUrl = "hdfs://master:9000/out/010_080_200_24_15_20/"
    val data = sc.textFile(inputUrl + "D.dat",numPartitions)
    val user = sc.textFile(inputUrl + "U.dat",numPartitions)
    
    val transactions = data.map(line => line.trim.split(" ").map(x => if (x == "") 0 else x.toInt).distinct)
    transactions.cache()
    
    //创建一个FPGrowth的算法实列
    //设置计算的最小支持度和数据分区
    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartitions)
    //获得FPGrowth计算结果
    val model = fpg.run(transactions)
    //收集频繁项集
    val fre = model.freqItemsets.map(itemset => (itemset.freq, itemset.items))
    fre.cache()
    
    //频繁项集升序排序
    val freq = fre.sortByKey().collect
    
    val resfreq = sc.makeRDD(freq.map(u => u._2.mkString(" ")))
    resfreq.saveAsTextFile(outputUrl+"freqItemsets")
    
    //推荐规则[前项, (后项, 置信度)]
    val AllAssociationRules = model.generateAssociationRules(minConfidence).map(x => (x.antecedent.toSet, (x.consequent.toSet, x.confidence)))
    
    //根据前项聚合产生的规则 [前项，[(后项，置信度)]]
    //不知道combineByKey是否平均分配到每个分区.repartition(numPartitions)
    val rulesList =AllAssociationRules.combineByKey(List(_), (x:List[(Set[Int], Double)], y:(Set[Int], Double)) => y :: x, (x:List[(Set[Int], Double)], y:List[(Set[Int], Double)]) => x ::: y)
    //保存关联规则与置信度[a,b] --> [ c : 0.9 ] | [ d : 0.8 ]
    rulesList.map(rule => (rule._1.mkString("[", ",", "]") + " --> " + rule._2.map(x => (x._1 + " : " + x._2.toString())).mkString("[ ", " | ", " ]"))).saveAsTextFile(outputUrl+"rulesList") 
    
    //
    //计算置信度最高的规则，置信度相同则合并
    val bestRules = rulesList.map(rule => (rule._1, rule._2.reduceLeft((a, b) => if(a._2 > b._2) a else if(a._2 == b._2) (a._1 ++ b._1, a._2) else b)))
    bestRules.cache()
    //保存关联规则中置信度最高的
    bestRules.map(rule => rule._1.mkString("[", ",", "]") + " --> " + rule._2._1.mkString("[", ",", "]") + " : " + rule._2._2).saveAsTextFile(outputUrl + "bestRules") 
    
    //收集，便于遍历
    val rules = bestRules.collect()
    //用户购买记录Array
    
    val userArray = user.map(line => line.trim.split(" ").map(_.toInt).distinct)
    userArray.cache()
    //计算用户推荐项
    val userRecItems = userArray.map(oneUser => (rules.filter(x => x._1.subsetOf(oneUser.toSet)).map(x => (x._2._1, x._2._2)), oneUser)).map(u => if(u._1.isEmpty) (Set(), u._2) else (u._1.reduce((x, y) => if(x._2 > y._2) x else if (x._2 == y._2) (x._1 ++ y._1, x._2) else y)._1.filter(x => !u._2.contains(x)), u._2))
    userRecItems.cache()
    
    //保存格式化
    val result = userRecItems.map(item => (item._1.mkString("[", ",", "]") +" for user : " + item._2.mkString("[", ",", "]")))
    result.cache()
    result.saveAsTextFile(outputUrl+"userRecItems")
    
    val allRules = rulesList.map(rule => (rule._1, rule._2.reduceLeft((a, b) => (a._1 ++ b._1, a._2))))
    allRules.cache()
    //保存关联规则中置信度最高的
    allRules.map(rule => rule._1.mkString("[", ",", "]") + " --> " + rule._2._1.mkString("[", ",", "]") + " : " + rule._2._2).saveAsTextFile(outputUrl + "allRules")
    
    val allrules = allRules.collect()
    
    val userAllRecItems = userArray.map(oneUser => (allrules.filter(x => x._1.subsetOf(oneUser.toSet)).map(x => (x._2._1, x._2._2)), oneUser)).map(u => if(u._1.isEmpty) (Set(), u._2) else (u._1.reduce((x, y) => if(x._2 > y._2) x else if (x._2 == y._2) (x._1 ++ y._1, x._2) else y)._1.filter(x => !u._2.contains(x)), u._2))
    userAllRecItems.cache()
    
    val allresult = userAllRecItems.map(item => (item._1.mkString("[", ",", "]")))
    allresult.cache()
    allresult.saveAsTextFile(outputUrl+"userAllRecItems")
    
    println("end of RecSys")
  }
}
