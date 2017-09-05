/**
 * *******************************************
 * Author	: Rutvij Mehta
 * Email	: rmehta4@ncsu.edu
 * Program	: assignment
 * Stack	: Scala,Apache Spark
 * *******************************************
 */

package org.gumgum

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.internal.Logging


object GumGumSpark {
  def main(args: Array[String]): Unit = {
    new SparkAssignment(args(0), args(1))
  }
}

object SparkLog extends Serializable {      
   @transient lazy val log = Logger.getLogger(getClass.getName)    
}

class SparkAssignment(val file1_path: String, val file2_path: String){

  val conf = new SparkConf().setAppName("GumGum Intern").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  val file1 = sc.textFile(file1_path)
  val file2 = sc.textFile(file2_path)

  //Removed timestamp from the line and added '{' to make json structure
  val eventJson = file1.map(line => line.split(", \\{")).map(line => {
    parse("{" + line(2))
  })

  val assetJson = file2.map(line => line.split(", \\{")).map(line => {
    parse("{" + line(2))
  })

  /*Search for pv and e keys from json and make a tuple
   * 
   * Corner cases
   * 1) if pv does not exist in the system
   * 2) if pv has null value
   */
  val eventData = eventJson.map(json => {
    implicit val formats = DefaultFormats
    (json \ "pv") match {
      case JString(data) => {
        ((json \ "pv").extract[String], (json \ "e").extract[String])
      }
      case JNothing => {
        SparkLog.log.info("No event key found for event :" + (json \ "e").extract[String])
        ("None", "None")
      }
      case JNull => {
         SparkLog.log.info("No event key found for event :" + (json \ "e").extract[String])
        ("None", "None")
      }
    }
  }).cache()

  eventData.count()
  
  
  //file1
  /* Keep the rows with 'click' and 'view' events only  
   * create the following tuple
   * 1) (key,1,0) for view
   * 2) (key,0,1) for click
   */
  val eventFilter = eventData.filter(data => (data._2 == "click" || data._2 == "view"))
  
  val countEvent = eventFilter.map(data => {
    if (data._2 == "view") {
      (data._1, 1, 0)
    } else {
      (data._1, 0, 1)
    }
  })
 
  
  /* sum and reduce the tupels with,
   * (key,(view,click))
   */
  val eventFinal = countEvent.
  map { case (key, view, click) => ((key), (view, click)) }.
  reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  
  
   /*Search for pv  from asset json and make a tuple
   * (pv,1) for each line where pv exists
   * Corner cases
   * 1) if pv does not exist in the system
   * 2) if pv has null value
   */

  val assetData = assetJson.map(json => {
    implicit val formats = DefaultFormats
    (json \ "pv") match {
      case JString(data) => {
        ((json \ "pv").extract[String], 1)
      }
      case JNull => {
        SparkLog.log.info("No asset key found")
        ("None", 1)
      }
      case JNothing => {
        SparkLog.log.info("No asset key found")
        ("None", 1)
      }
    }
  }).map(line => (line._1, (line._2)))

  //reduce and sum the total values by key
  val assetFinal = assetData.reduceByKey(_ + _)
  

  //Join asset and event tuples to combine the event and asset impressions
  val combinedResult = assetFinal.join(eventFinal)
  
  val finalResult = combinedResult.map(x => (x._1, x._2._1, x._2._2._1, x._2._2._2)).
  map(_.productIterator.mkString("\t"))
  
  finalResult.cache()
  finalResult.count()
   
  //Store it to Downloads directory of Users
  // Using coalesce because of the limited amount of data else
  // would have used bash script or other means to merge the file
  try {
    finalResult.saveAsTextFile(System.getProperty("user.home") + "/Downloads/Spark_Rutvij")
  }
  catch{
    case e:FileAlreadyExistsException => {
       SparkLog.log.info("******************************************************************")
       SparkLog.log.info("******************************************************************")
       SparkLog.log.error("Please delete/rename the existing output folder \"Spark Rutvij\" from Downloads diectory")
       SparkLog.log.info("******************************************************************")
       SparkLog.log.info("******************************************************************")
    }
    System.exit(0)
  }
  SparkLog.log.info("******************************************************************")
  SparkLog.log.info("******************************************************************")
  SparkLog.log.info("Output folder generated in " + System.getProperty("user.home") + "/Downloads/Spark_Rutvij")
  SparkLog.log.info("******************************************************************")
  SparkLog.log.info("******************************************************************")
}
