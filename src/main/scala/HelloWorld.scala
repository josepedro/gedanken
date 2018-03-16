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
    val number_lines = preprocessor.sizeVertical;
    val number_rows = preprocessor.sizeAxial;


    var toStreamDirection = sc.textFile("output_mr0/input.txt")

    toStreamDirection = toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last.toDouble)
      var density = fis.reduce((a,b) => a + b)

      val c = 1 / Math.sqrt(3)
      val c_square = c * c
      val omega = 1.9
      val resultCollide = ""
      val f1 = 3.0
      val f2 = 4.5
      val f3 = 1.5
      val w0 = (16.0 / 36.0).toDouble
      val w1 = (4.0 / 36.0).toDouble
      val w2 = (1.0 / 36.0).toDouble
      val rt0 = density*w0
      val rt1 = density*w1
      val rt2 = density*w2
      val ciX = Array(0, 0, +c, +c, +c, 0, -c, -c, -c)
      val ciY = Array(0, +c, +c, 0, -c, -c, -c, 0, +c)
      var velocityX : Double = 0
      var velocityY : Double = 0
      for (i <- 1 to 9){
        velocityX = fis.take(i).last*ciX(i-1) + velocityX
        velocityY = fis.take(i).last*ciY(i-1) + velocityY
      }
      velocityX = velocityX/density
      velocityY = velocityY/density

      val uxsq = velocityX*velocityX;
      val uysq = velocityY*velocityY;
      val usq = uxsq + uysq;
      var result = ""
      for (i <- 1 to 9){
        var fi = fis(i - 1)
        var fiEq = 6666.6666;
        if (i - 1 == 0)
          fiEq = rt0*(1 - f3*usq)
        else if (i - 1 == 3)
          fiEq = rt1*(1 + f1*velocityX +f2*uxsq -f3*usq)
        else if (i - 1 == 5)
          fiEq = rt1*(1 + f1*velocityY +f2*uysq -f3*usq)
        else if (i - 1 == 7)
          fiEq = rt1*(1 - f1*velocityX +f2*uxsq -f3*usq)
        else if (i - 1 == 1)
          fiEq = rt1*(1 - f1*velocityY +f2*uysq -f3*usq)
        else if (i - 1 == 2)
          fiEq = rt2*(1 + f1*(velocityX + velocityY) + f2*(velocityX + velocityY)*(velocityX + velocityY) - f3*usq)
        else if (i - 1 == 4)
          fiEq = rt2*(1 + f1*(-velocityX + velocityY) + f2*(-velocityX + velocityY)*(-velocityX + velocityY) - f3*usq)
        else if (i - 1 == 6)
          fiEq = rt2*(1 + f1*(-velocityX - velocityY) + f2*(-velocityX - velocityY)*(-velocityX - velocityY) - f3*usq)
        else if (i - 1 == 8)
          fiEq = rt2*(1 + f1*(velocityX - velocityY) + f2*(velocityX - velocityY)*(velocityX - velocityY) - f3*usq)

        fi = (1 - omega)*fi + omega*fiEq
        result += " " + (i - 1).toString + ":" + fi.toString
      }
      val id = idDirectionsFis.head
      (id + result)
    })


    toStreamDirection = toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      (id + " " + directions.take(1).last + ":" + fis.take(1).last)
    }).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get1_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(2).last + ":" + fis.take(2).last)
    })).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get2_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(3).last + ":" + fis.take(3).last)
    })).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get3_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(4).last + ":" + fis.take(4).last)
    })).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get4_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(5).last + ":" + fis.take(5).last)
    })).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get5_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(6).last + ":" + fis.take(6).last)
    })).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get6_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(7).last + ":" + fis.take(7).last)
    })).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get7_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(8).last + ":" + fis.take(8).last)
    })).union(toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last)
      val directions = directionsFis.map(element => element.split(":").head)
      val id = idDirectionsFis.head
      val newId = StreamD2Q9.get8_id(id.toInt, number_lines, number_rows).toString
      (newId + " " + directions.take(9).last + ":" + fis.take(9).last)
    })).map(line => (line.split(" ").head, line.split(" ").last)).reduceByKey(_+ " " + _).map(line => line._1 + " " + line._2)



    val idDensities = toStreamDirection.map({ line =>
      val idDirectionsFis = line.split(" ")
      val directionsFis =  idDirectionsFis.slice(1, idDirectionsFis.size)
      val fis = directionsFis.map(element => element.split(":").last.toDouble)
      val density = fis.reduce((a,b) => a + b)
      val id = idDirectionsFis.head
      id + " " + density.toString
    })


    idDensities.coalesce(1).saveAsTextFile("/home/pedro/git/spark-hello-world/final_result")
    val posprocessor = new Posprocessor("/home/pedro/git/spark-hello-world/final_result" + "/part-00000",
      "/home/pedro/git/spark-hello-world/final_result" + "/outputFinal.txt")
    posprocessor.posprocess()
    //idDensities.foreach(println)

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

