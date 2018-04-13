package com.zyh.t0413
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 
object RecSysMain {
   def main(args: Array[String]): Unit = {
    val conf = new SparkConf() 
    conf.setAppName("RecSys-SPARKAPP") 
        .setMaster("spark://master:7077")
        .set("spark.executor.memory", "6g")
        .set("spark.executor.cores", "2")
        .set("spark.driver.memory", "24g")
        .set("spark.driver.cores", "4")
    val sc = new SparkContext(conf) 
    sc.addJar("/home/yuehui/git/RecSys/bin/RecSys.jar")
    //最小支持度
    val minSupport = 0.125
    //最小置信度
    val minConfidence = 0.80
    //数据分区数
    val numPartitions = 120
    //hdfs input url 
    val inputUrl = "hdfs://master:9000/data/"
    //hdfs out url 
    val outputUrl = "hdfs://master:9000/out/0413_sim/"
    //读取数据
    val data = sc.textFile(inputUrl + "D.dat",numPartitions)
    val user = sc.textFile(inputUrl + "U.dat",numPartitions)
    //转换为Int数组，并去重，更节省内存空间
    val transactions = data.map(line => line.trim.split(" ").map(_.toInt).distinct)
    transactions.cache()
    //用户购买记录Array
    val userArray = user.map(line => line.trim.split(" ").map(_.toInt).distinct)
    userArray.cache()
    val usrSet = userArray.map(_.toSet)
    
    //创建一个FPGrowth的算法实列,设置计算的最小支持度和数据分区
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
    val AllAssociationRules = model.generateAssociationRules(minConfidence).map(x => (x.antecedent.toSet, (x.consequent, x.confidence)))
    //根据前项聚合产生的规则 [前项，[(后项，置信度)]]
    //不知道combineByKey是否平均分配到每个分区.repartition(numPartitions)
    val rulesList =AllAssociationRules.combineByKey(List(_), (x:List[(Array[Int], Double)], y:(Array[Int], Double)) => y :: x, (x:List[(Array[Int], Double)], y:List[(Array[Int], Double)]) => x ::: y)
    //保存关联规则与置信度[a,b] --> [ c : 0.9 ] | [ d : 0.8 ]
    rulesList.map(rule => (rule._1.mkString("[", ",", "]") + " --> " + rule._2.map(x => (x._1 + " : " + x._2.toString())).mkString("[ ", " | ", " ]"))).saveAsTextFile(outputUrl+"rulesList2") 
    
    //计算最高的推荐项
    //计算置信度最高的规则，置信度相同则合并
    val bestRules = rulesList.map(rule => (rule._1, rule._2.reduceLeft((a, b) => if(a._2 > b._2) a else if(a._2 == b._2) (a._1 ++ b._1, a._2) else b)))
    bestRules.cache()
    //保存关联规则中置信度最高的
    bestRules.map(rule => rule._1.mkString("[", ",", "]") + " --> " + rule._2._1.mkString("[", ",", "]") + " : " + rule._2._2).saveAsTextFile(outputUrl + "bestRules") 
    //收集，用于遍历
    val rules = bestRules.collect()
    
    
    //计算用户推荐项
    val userRecItems = usrSet.map(uSet => (rules.filter(x => x._1.subsetOf(uSet)).map(x => (x._2._1.toSet, x._2._2)), uSet)).map(u => if(u._1.isEmpty) (Set(), u._2) else (u._1.reduce((x, y) => if(x._2 > y._2) x else if (x._2 == y._2) (x._1 ++ y._1, x._2) else y)._1.filter(x => !u._2.contains(x)), u._2))
   
    //计算用户推荐项
//    val userRecItems = userArray.map(oneUser => (rules.filter(x => x._1.subsetOf(oneUser.toSet)).map(x => x._2._1).flatMap(f => f).distinct.filter(e => !oneUser.contains(e)), oneUser))
    userRecItems.cache()
    //保存格式化
    val result = userRecItems.map(item => (item._1.mkString("[", ",", "]")))
    result.cache()
    result.saveAsTextFile(outputUrl+"userRecItemsCom")
    
    println("end of RecSys")   
       
    //计算非聚合: 关联规则置信度到达minConfigdence即算作推荐项
    //并不需要聚合最高的RDD[(Set(前项),Array(后项))]
    val allRules = rulesList.map(rule => (rule._1, rule._2.reduceLeft((a, b) => (a._1 ++ b._1, a._2))._1))
    allRules.cache()
    //保存关联规则中置信度最高的
    allRules.map(rule => rule._1.mkString("[", ",", "]") + " --> " + rule._2.mkString("[", ",", "]")).saveAsTextFile(outputUrl + "allRules")
    val allrulesArray = allRules.collect()
   // val userAllRecItems = userArray.map(oneUser => (allrulesArray.filter(x => x._1.subsetOf(oneUser.toSet)).flatMap(x => x._2).distinct.filter(e => !oneUser.contains(e)), oneUser))
    
    val userAllRecItems = usrSet.map(oneUser => (allrulesArray.filter(x => x._1.subsetOf(oneUser)), oneUser)).map(u => if(u._1.isEmpty) (Array(), u._2) else (u._1.flatMap(x => x._2).distinct , u._2))
    
    userAllRecItems.cache()
    val allresult = userAllRecItems.map(item => (item._1.mkString("[", ",", "]")))
    allresult.cache()
    allresult.saveAsTextFile(outputUrl+"userAllRecItemsCom")
    
    
   }
}