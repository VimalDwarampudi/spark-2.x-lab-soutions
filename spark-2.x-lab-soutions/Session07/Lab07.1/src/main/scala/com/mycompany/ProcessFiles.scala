/*
 * This code is sample code, provided as-is, and we make no
 * warranties as to its correctness or suitability for
 * any purpose.
 *
 * We hope that it's useful to you.  Enjoy.
 * Copyright LearningPatterns Inc.
 */

package com.mycompany

import org.apache.spark.SparkContext
// DONE: Import the SparkSession and functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/*
Usage:
spark-submit  --master spark://localhost:7077  target/scala-2.11/myapp_2.11-1.0.jar    <files to process>

Multiple files can be specified with a space between the file paths.
Examples of files are:
                            /etc/hosts
                            ~/spark-labs/data/twinkle/500M.data
                            s3n://elephantscale-public/data/twinkle/100M.data
                            tachyon://tachyon_ip_address:19998/file

e.g:
- with 4G executor memory and turning off verbose logging
    spark-submit --class com.mycompany.ProcessFiles  --master spark://localhost:7077 --executor-memory 4g   --driver-class-path logging/   target/scala-2.11/myapp_2.11-1.0.jar  ~/spark-labs/data/twinkle/1G.data

*/


object ProcessFiles {
  def main(args: Array[String]) {
    if (args.length < 1) {
        println ("need file(s) to load")
        System.exit(1)
    }

    // DONE: Create a SparkSession
    val spark = SparkSession.builder.appName("Process Files").getOrCreate()
    // We'll set master from spark-submit, so don't set it here.

    var file = ""
    for (file <- args) { // looping over file args
      // DONE: create a DataFrame from the file (assume it is text)
      val fileDF = spark.read.text(file)
      // DONE: count # of elements in the dataframe.
      val count =  fileDF.count

       println("### %s: count:  %,d ".format(file, count))
      }

      // Do this so the 4040 UI stays alive - bit of a hack, but useful for us.
      println("### Hit enter to terminate the program...:")
      val line = Console.readLine
   }
}
