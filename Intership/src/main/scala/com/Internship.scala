package com.sparkTutorial.Aayush

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Internship {

  def my_map1(f: String): (String, String) = {
    val temp = f.split(",")
    (temp(0), temp(1))
  }

  def my_map2(f: String): (String, Integer) = {
    val temp = f.split(",")
    (temp(0), Integer.valueOf(temp(1)))
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val con = new SparkConf().setMaster("local[*]").setAppName("inern");
    val sc = new SparkContext(con);



    val genchanA = sc.textFile("Input/join2_genchanA.txt");
    val genchanB = sc.textFile("Input/join2_genchanB.txt");
    val genchanC = sc.textFile("Input/join2_genchanC.txt");
    val gennumA = sc.textFile("Input/join2_gennumA.txt");
    val gennumB = sc.textFile("Input/join2_gennumB.txt");
    val gennumC = sc.textFile("Input/join2_gennumC.txt");

    val genchan = genchanA.union(genchanB).union(genchanC);
    val gennum = gennumA.union(gennumB).union(gennumC);

    //Task 1


    println("Total number of viewers for show on ABC")

    val filteABC = genchan.filter(f => f.split(",")(1) == "ABC")

    val getchanrdd = filteABC.map(f => my_map1(f))
    val getnumrdd = gennum.map(f => my_map2(f))
    val joinedData = getchanrdd.join(getnumrdd);
    val j1 = joinedData.map(f => (f._1, f._2._2));
    val ans1 = j1.reduceByKey((x, y) => x + y)
    for ((c, n) <- ans1.collect()) println(c + " " + n)
    println();

    println("Task 2")


    println("Number of viewers for the on BAT")
    //Task 2

    val filteABC1 = genchan.filter(f => f.split(",")(1) == "BAT")

    val getchanrdd1 = filteABC1.map(f => my_map1(f))
    val getnumrdd1 = gennum.map(f => my_map2(f))

    val joinedData1 = getchanrdd1.join(getnumrdd);
    val j11 = joinedData.map(f => (f._1, f._2._2));

    val totalViewer1 = j11.reduceByKey((x, y) => Integer.valueOf(x) + Integer.valueOf(y));
    val out = totalViewer1.map(_._2).reduce(_ + _);
    println(out);
    println();
    println("Task 3")

    println("Most Viewed show on ABC channel")
    // Task 3
    val totalViewer = j1.reduceByKey((x, y) => x + y);
    val all = totalViewer.map(_._2).reduce((x, y) => if (x > y) x else y);
    val smax = totalViewer.filter(_._2 == all).map(_._1);
    for ((x) <- smax.collect()) println(x)

    println();
    println("Task 4")
    println("The aired show on ZOO , NOX , ABC")
    //Task 4


    val abc = genchan.filter(f => f.split(",")(1) == "ZOO" || f.split(",")(1) == "NOX" || f.split(",")(1) == "ABC")
    val prdd = abc.map(f => f.split(",")(0)).distinct()
    for (bedrooms <- prdd.collect()) println(bedrooms)

  }


}
