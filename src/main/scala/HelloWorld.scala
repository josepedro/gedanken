// import required spark classes
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.io.Source
import java.io._
import scala.collection.mutable.ArrayBuffer

/*
    8  1  2
    7  0  3
    6  5  4
*/

// define main method (Spark entry point)
object HelloWorld {

    def get1( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if(line == 0 && row == 0){
            return Array(numberLines - 1, row)
        }
        else if(line == 0 && row == numberRows - 1){
            return Array(numberLines - 1, numberRows - 1)
        }
        else if(line == 0 && row > 0 && row < numberRows - 1){
            return Array(numberLines - 1, row)
        }
         else {
            return Array(line - 1, row)
        }
    }

    def get2( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if(line == 0 && row == 0){
            return Array(numberLines - 1, row + 1)
        }
        else if(line == 0 && row == numberRows - 1){
            return Array(numberLines - 1, 0)
        }
        else if(line == numberLines - 1 && row == numberRows - 1){
            return Array(numberLines - 2, 0)
        }
        else if(line == 0 && row > 0 && row < numberRows - 1){
            return Array(numberLines - 1, row + 1)
        }
        else if(line > 0 && line < numberLines - 1 && row == numberRows - 1){
            return Array(line - 1, 0)
        }
         else {
            return Array(line - 1, row + 1)
        }
    }

    def get3( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if(line == 0 && row == numberRows - 1){
            return Array(line, 0)
        }
        else if(line == numberLines - 1 && row == numberRows - 1){
            return Array(numberLines - 1, 0)
        }
        else if(line > 0 && line < numberLines -1 && row == numberRows - 1){
            return Array(line, 0)
        }
        else {
            return Array(line, row + 1)
        }
    }

    def get4( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if (line == 0 && row == numberRows - 1){
            return Array(1, 0)
        }
        else if(line == numberLines - 1 && row == numberRows - 1){
            return Array(0, 0)
        }
        else if(line == numberLines - 1 && row == 0){
            return Array(0, 1)
        }
        else if(line > 0 && line < numberLines - 1 && row == numberRows - 1){
            return Array(line + 1, 0)
        }
        else if(line == numberLines - 1 && row > 0 && row < numberRows - 1){
            return Array(0, row + 1)
        }
         else {
            return Array(line + 1, row + 1)
        }
    }

    def get5( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if(line == numberLines - 1 && row == 0){
            return Array(0, row)
        }
        else if(line == numberLines - 1 && row == numberRows - 1){
            return Array(0, row)
        }
        else if(line == numberLines - 1 && row > 0 && row < numberRows - 1){
            return Array(0, row)
        }
        else {
            return Array(line + 1, row)
        }
    }

    def get6( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if (line == 0 && row == 0){
            return Array(line + 1, numberRows - 1)
        }
        else if(line ==  numberLines - 1 && row == 0){
            return Array(0, numberRows - 1)
        }
        else if(line == numberLines - 1 && row == numberRows - 1){
            return Array(0, row - 1)
        }
        else if(line > 0 && line < numberLines - 1 && row == 0){
            return Array(line + 1, numberRows - 1)
        }
        else if(line == numberLines - 1 && row > 0 && row < numberRows - 1){
            return Array(0, row - 1)
        }
         else {
            return Array(line + 1, row - 1)
        }
    }

    def get7( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if(line == 0 && row == 0){
            return Array(line, numberRows - 1)
        }
        else if(line == numberLines - 1 && row == 0){
            return Array(line, numberRows - 1)
        }
        else if(line > 0 && line < numberLines - 1 && row == 0){
            return Array(line, numberRows - 1)
        }
        else {
            return Array(line, row - 1)
        }
    }

    def get8( line:Int, row:Int, numberLines:Int, numberRows:Int) : Array[Int] = {
        if (line == 0 && row == 0){
            return Array(numberLines - 1, numberRows - 1)
        }
        else if(line == 0 && row == numberRows - 1){
            return Array(numberLines - 1, row - 1)
        }
        else if(line == numberLines - 1 && row == 0){
            return Array(line - 1, numberRows - 1)
        }
        else if(line > 0 && line < numberLines - 1 && row == 0){
            return Array(line - 1, numberRows - 1)
        }
        else if(line == 0 && row > 0 && row < numberRows - 1){
            return Array(numberLines - 1, row - 1)
        }
         else {
            return Array(line - 1, row - 1)
        }
    }

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local[2]").set("spark.executor.memory","1g");
    val sc = new SparkContext(conf)

    val inputFile = "/home/pedro/git/spark-hello-world/inputData.txt"
    val inputFileMR = "/home/pedro/git/spark-hello-world/output_mr0/input.txt"
    Runtime.getRuntime.exec("mkdir /home/pedro/git/spark-hello-world/output_mr0")

    val preprocessor = new Preprocessor(inputFile, inputFileMR)
    preprocessor.generateDefaultMesh()
    preprocessor.preprocess()



    println("Resposta =======================")

    // do stuff
    println("************")
    println("************")
    println("Ok!")
    println("************")
    println("************")
    
    // terminate spark context
    sc.stop()
    
  }
}

