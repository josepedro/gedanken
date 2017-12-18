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
    val conf = new SparkConf().setAppName("HelloWorld")
    val sc = new SparkContext(conf)

    val filename = "mesh_3_h.txt"
    var numberRows = scala.io.Source.fromFile(filename).getLines.toArray.map(_.split(" ")).length
    var numberLines = scala.io.Source.fromFile(filename).getLines.toArray.length
    var matrixElements = scala.io.Source.fromFile(filename).getLines.toArray.map(_.split(" "))

    var arrayCells = ArrayBuffer[(Long, scala.collection.immutable.Map[String,Double])]()
    var arrayRelationships = ArrayBuffer[org.apache.spark.graphx.Edge[String]]()
    var elementId = 0
    // in y
    for( line <- 0 to numberLines - 1){
        // in x
        for( row <- 0 to numberRows - 1) {
            elementId += 1
            var directionsDensity:Map[String,Double] = Map()
            for( direction <- 0 to 8) {
                var density = matrixElements(line)(row).toDouble/9.0
                directionsDensity += (direction.toString -> density)
            }
            directionsDensity += ("ux" -> 0.0)
            directionsDensity += ("uy" -> 0.0)
            var cell = (elementId.toLong, directionsDensity)
            arrayCells += cell
            matrixElements(line)(row) = elementId.toString
        }
    }

    var positions = Array(0,0)
    elementId = 0
    for( line <- 0 to numberLines - 1){
        for( row <- 0 to numberRows - 1) {
            elementId += 1
            positions = get1(line,row,numberLines,numberRows) //(y, x)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "1")
            positions = get2(line,row,numberLines,numberRows)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "2")
            positions = get3(line,row,numberLines,numberRows)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "3")
            positions = get4(line,row,numberLines,numberRows)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "4")
            positions = get5(line,row,numberLines,numberRows)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "5")
            positions = get6(line,row,numberLines,numberRows)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "6")
            positions = get7(line,row,numberLines,numberRows)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "7")
            positions = get8(line,row,numberLines,numberRows)
            arrayRelationships += Edge(elementId.toLong, matrixElements(positions(0))(positions(1)).toLong, "8")
        }
    }    

    var cellsStreamed = sc.parallelize(arrayCells) 

    var relationshipCells = sc.parallelize(arrayRelationships)

    var latticeBefore = Graph(cellsStreamed, relationshipCells)

    println("Resposta =======================")
    val cellsResultBefore = latticeBefore.mapVertices((id, attr) =>  attr("1") +
    attr("2") + attr("3")
   + attr("4")
   + attr("5")
   + attr("6")
   + attr("7")
   + attr("8") 
   + attr("0"))
//val cellsResultBefore = latticeBefore.mapVertices((id, attr) =>  attr("ux"))
    cellsResultBefore.vertices.saveAsTextFile("Before.txt")
    println("Resposta =======================")

    for( a <- 1 to 20){
        /*
            8  1  2
            7  0  3
            6  5  4
        */

        // Stream and collide things
         cellsStreamed = 
            cellsStreamed.map(cell => {  
            val rho: Double = cell._2("1") +
                          cell._2("2") +
                          cell._2("3") +
                          cell._2("4") +
                          cell._2("5") +
                          cell._2("6") + 
                          cell._2("7") +
                          cell._2("8") +
                          cell._2("0")
                    val ux: Double = cell._2("ux")
                    val uy: Double = cell._2("uy")
                    val c: Double = 1/scala.math.sqrt(3)
                    val c0: Array[Double] = Array(0, 0)
                    val c_square: Double = c*c 
                    val omega: Double =  1.98
                    val epsilon_0: Double = 16/36
                    val term_1: Double = (c0(0)*uy + c0(1)*ux)/c_square
                    val term_2: Double = ((c0(0)*uy + c0(1)*ux)*(c0(0)*uy + c0(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                    val f_0_eq: Double = rho*epsilon_0*(1 + term_1 + term_2)
                    var f_0 = cell._2("0")
                    f_0 = f_0 - omega*(f_0 - f_0_eq)

                (cell._1, Map("1" -> cell._2("1"),
                    "2" -> cell._2("2"),
                    "3" -> cell._2("3"),
                    "4" -> cell._2("4"),
                    "5" -> cell._2("5"),
                    "6" -> cell._2("6"),
                    "7" -> cell._2("7"),
                    "8" -> cell._2("8"),
                    "0" -> f_0,
                    "ux" -> ux,
                    "uy" -> uy  ))
            })
            latticeBefore = Graph(cellsStreamed, relationshipCells)

        cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if (tripletFields.attr == "1"){
                val rho: Double = tripletFields.srcAttr("1") +
                      tripletFields.srcAttr("2") +
                      tripletFields.srcAttr("3") +
                      tripletFields.srcAttr("4") +
                      tripletFields.srcAttr("5") +
                      tripletFields.srcAttr("6") + 
                      tripletFields.srcAttr("7") +
                      tripletFields.srcAttr("8") +
                      tripletFields.srcAttr("0")
                val ux: Double = tripletFields.srcAttr("ux")
                val uy: Double = tripletFields.srcAttr("uy")
                val c: Double = 1/scala.math.sqrt(3)
                val c1: Array[Double] = Array(+c, 0)
                val c_square: Double = c*c 
                val omega: Double =  1.98
                val epsilon_1: Double = 4/36
                val term_1: Double = (c1(0)*uy + c1(1)*ux)/c_square
                val term_2: Double = ((c1(0)*uy + c1(1)*ux)*(c1(0)*uy + c1(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                val f_1_eq: Double = rho*epsilon_1*(1 + term_1 + term_2)
                var f_1 = tripletFields.srcAttr("1")
                f_1 = f_1 - omega*(f_1 - f_1_eq)

                tripletFields.sendToDst(Map(
                    "1" -> f_1,
                    "2" -> tripletFields.dstAttr("2"),
                    "3" -> tripletFields.dstAttr("3"),
                    "4" -> tripletFields.dstAttr("4"),
                    "5" -> tripletFields.dstAttr("5"),
                    "6" -> tripletFields.dstAttr("6"),
                    "7" -> tripletFields.dstAttr("7"),
                    "8" -> tripletFields.dstAttr("8"),
                    "0" -> tripletFields.dstAttr("0"),
                    "ux" -> tripletFields.dstAttr("ux"),
                    "uy" -> tripletFields.dstAttr("uy")
                ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

        cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if(tripletFields.attr == "2"){
                val rho: Double = tripletFields.srcAttr("1") +
                      tripletFields.srcAttr("2") +
                      tripletFields.srcAttr("3") +
                      tripletFields.srcAttr("4") +
                      tripletFields.srcAttr("5") +
                      tripletFields.srcAttr("6") + 
                      tripletFields.srcAttr("7") +
                      tripletFields.srcAttr("8") +
                      tripletFields.srcAttr("0")
                val ux: Double = tripletFields.srcAttr("ux")
                val uy: Double = tripletFields.srcAttr("uy")
                val c: Double = 1/scala.math.sqrt(3)
                val c2: Array[Double] = Array(+c, +c)
                val c_square: Double = c*c 
                val omega: Double =  1.98
                val epsilon_2: Double = 1/36
                val term_1: Double = (c2(0)*uy + c2(1)*ux)/c_square
                val term_2: Double = ((c2(0)*uy + c2(1)*ux)*(c2(0)*uy + c2(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                val f_2_eq: Double = rho*epsilon_2*(1 + term_1 + term_2)
                var f_2 = tripletFields.srcAttr("2")
                f_2 = f_2 - omega*(f_2 - f_2_eq)

                    tripletFields.sendToDst(Map(
                        "1" -> tripletFields.dstAttr("1"),
                        "2" -> f_2,
                        "3" -> tripletFields.dstAttr("3"),
                        "4" -> tripletFields.dstAttr("3"),
                        "5" -> tripletFields.dstAttr("5"),
                        "6" -> tripletFields.dstAttr("6"),
                        "7" -> tripletFields.dstAttr("7"),
                        "8" -> tripletFields.dstAttr("8"),
                        "0" -> tripletFields.dstAttr("0"),
                        "ux" -> tripletFields.dstAttr("ux"),
                        "uy" -> tripletFields.dstAttr("uy")
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)   

        cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if(tripletFields.attr == "3"){
                val rho: Double = tripletFields.srcAttr("1") +
                  tripletFields.srcAttr("2") +
                  tripletFields.srcAttr("3") +
                  tripletFields.srcAttr("4") +
                  tripletFields.srcAttr("5") +
                  tripletFields.srcAttr("6") + 
                  tripletFields.srcAttr("7") +
                  tripletFields.srcAttr("8") +
                  tripletFields.srcAttr("0")
                val ux: Double = tripletFields.srcAttr("ux")
                val uy: Double = tripletFields.srcAttr("uy")
                val c: Double = 1/scala.math.sqrt(3)
                val c3: Array[Double] = Array(0, +c)
                val c_square: Double = c*c 
                val omega: Double =  1.98
                val epsilon_3: Double = 4/36
                val term_1: Double = (c3(0)*uy + c3(1)*ux)/c_square
                val term_2: Double = ((c3(0)*uy + c3(1)*ux)*(c3(0)*uy + c3(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                val f_3_eq: Double = rho*epsilon_3*(1 + term_1 + term_2)
                var f_3 = tripletFields.srcAttr("3")
                f_3 = f_3 - omega*(f_3 - f_3_eq)

                    tripletFields.sendToDst(Map(
                        "1" -> tripletFields.dstAttr("1"),
                        "2" -> tripletFields.dstAttr("2"),
                        "3" -> f_3,
                        "4" -> tripletFields.dstAttr("4"),
                        "5" -> tripletFields.dstAttr("5"),
                        "6" -> tripletFields.dstAttr("6"),
                        "7" -> tripletFields.dstAttr("7"),
                        "8" -> tripletFields.dstAttr("8"),
                        "0" -> tripletFields.dstAttr("0"),
                        "ux" -> tripletFields.dstAttr("ux"),
                        "uy" -> tripletFields.dstAttr("uy")
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if(tripletFields.attr == "4"){
                val rho: Double = tripletFields.srcAttr("1") +
                      tripletFields.srcAttr("2") +
                      tripletFields.srcAttr("3") +
                      tripletFields.srcAttr("4") +
                      tripletFields.srcAttr("5") +
                      tripletFields.srcAttr("6") + 
                      tripletFields.srcAttr("7") +
                      tripletFields.srcAttr("8") +
                      tripletFields.srcAttr("0")
                val ux: Double = tripletFields.srcAttr("ux")
                val uy: Double = tripletFields.srcAttr("uy")
                val c: Double = 1/scala.math.sqrt(3)
                val c4: Array[Double] = Array(-c, +c)
                val c_square: Double = c*c 
                val omega: Double =  1.98
                val epsilon_4: Double = 1/36
                val term_1: Double = (c4(0)*uy + c4(1)*ux)/c_square
                val term_2: Double = ((c4(0)*uy + c4(1)*ux)*(c4(0)*uy + c4(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                val f_4_eq: Double = rho*epsilon_4*(1 + term_1 + term_2)
                var f_4 = tripletFields.srcAttr("4")
                f_4 = f_4 - omega*(f_4 - f_4_eq)

                    tripletFields.sendToDst(Map(
                        "1" -> tripletFields.dstAttr("1"),
                        "2" -> tripletFields.dstAttr("2"),
                        "3" -> tripletFields.dstAttr("3"),
                        "4" -> f_4,
                        "5" -> tripletFields.dstAttr("5"),
                        "6" -> tripletFields.dstAttr("6"),
                        "7" -> tripletFields.dstAttr("7"),
                        "8" -> tripletFields.dstAttr("8"),
                        "0" -> tripletFields.dstAttr("0"),
                        "ux" -> tripletFields.dstAttr("ux"),
                        "uy" -> tripletFields.dstAttr("uy")
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

        cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if(tripletFields.attr == "5"){
                val rho: Double = tripletFields.srcAttr("1") +
                      tripletFields.srcAttr("2") +
                      tripletFields.srcAttr("3") +
                      tripletFields.srcAttr("4") +
                      tripletFields.srcAttr("5") +
                      tripletFields.srcAttr("6") + 
                      tripletFields.srcAttr("7") +
                      tripletFields.srcAttr("8") +
                      tripletFields.srcAttr("0")
                val ux: Double = tripletFields.srcAttr("ux")
                val uy: Double = tripletFields.srcAttr("uy")
                val c: Double = 1/scala.math.sqrt(3)
                val c5: Array[Double] = Array(-c, 0)
                val c_square: Double = c*c 
                val omega: Double =  1.98
                val epsilon_5: Double = 1/36
                val term_1: Double = (c5(0)*uy + c5(1)*ux)/c_square
                val term_2: Double = ((c5(0)*uy + c5(1)*ux)*(c5(0)*uy + c5(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                val f_5_eq: Double = rho*epsilon_5*(1 + term_1 + term_2)
                var f_5 = tripletFields.srcAttr("5")
                f_5 = f_5 - omega*(f_5 - f_5_eq)

                    tripletFields.sendToDst(Map(
                        "1" -> tripletFields.dstAttr("1"),
                        "2" -> tripletFields.dstAttr("2"),
                        "3" -> tripletFields.dstAttr("3"),
                        "4" -> tripletFields.dstAttr("3"),
                        "5" -> f_5,
                        "6" -> tripletFields.dstAttr("6"),
                        "7" -> tripletFields.dstAttr("7"),
                        "8" -> tripletFields.dstAttr("8"),
                        "0" -> tripletFields.dstAttr("0"),
                        "ux" -> tripletFields.dstAttr("ux"),
                        "uy" -> tripletFields.dstAttr("uy")
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if(tripletFields.attr == "6"){
                    val rho: Double = tripletFields.srcAttr("1") +
                      tripletFields.srcAttr("2") +
                      tripletFields.srcAttr("3") +
                      tripletFields.srcAttr("4") +
                      tripletFields.srcAttr("5") +
                      tripletFields.srcAttr("6") + 
                      tripletFields.srcAttr("7") +
                      tripletFields.srcAttr("8") +
                      tripletFields.srcAttr("0")
                    val ux: Double = tripletFields.srcAttr("ux")
                    val uy: Double = tripletFields.srcAttr("uy")
                    val c: Double = 1/scala.math.sqrt(3)
                    val c6: Array[Double] = Array(-c, -c)
                    val c_square: Double = c*c 
                    val omega: Double =  1.98
                    val epsilon_6: Double = 1/36
                    val term_1: Double = (c6(0)*uy + c6(1)*ux)/c_square
                    val term_2: Double = ((c6(0)*uy + c6(1)*ux)*(c6(0)*uy + c6(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                    val f_6_eq: Double = rho*epsilon_6*(1 + term_1 + term_2)
                    var f_6 = tripletFields.srcAttr("6")
                    f_6 = f_6 - omega*(f_6 - f_6_eq)

                    tripletFields.sendToDst(Map(
                        "1" -> tripletFields.dstAttr("1"),
                        "2" -> tripletFields.dstAttr("2"),
                        "3" -> tripletFields.dstAttr("3"),
                        "4" -> tripletFields.dstAttr("4"),
                        "5" -> tripletFields.dstAttr("5"),
                        "6" -> f_6,
                        "7" -> tripletFields.dstAttr("7"),
                        "8" -> tripletFields.dstAttr("8"),
                        "0" -> tripletFields.dstAttr("0"),
                        "ux" -> tripletFields.dstAttr("ux"),
                        "uy" -> tripletFields.dstAttr("uy")
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)


         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if(tripletFields.attr == "7"){
                    val rho: Double = tripletFields.srcAttr("1") +
                      tripletFields.srcAttr("2") +
                      tripletFields.srcAttr("3") +
                      tripletFields.srcAttr("4") +
                      tripletFields.srcAttr("5") +
                      tripletFields.srcAttr("6") + 
                      tripletFields.srcAttr("7") +
                      tripletFields.srcAttr("8") +
                      tripletFields.srcAttr("0")
                    val ux: Double = tripletFields.srcAttr("ux")
                    val uy: Double = tripletFields.srcAttr("uy")
                    val c: Double = 1/scala.math.sqrt(3)
                    val c7: Array[Double] = Array(-c, 0)
                    val c_square: Double = c*c 
                    val omega: Double =  1.98
                    val epsilon_7: Double = 4/36
                    val term_1: Double = (c7(0)*uy + c7(1)*ux)/c_square
                    val term_2: Double = ((c7(0)*uy + c7(1)*ux)*(c7(0)*uy + c7(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                    val f_7_eq: Double = rho*epsilon_7*(1 + term_1 + term_2)
                    var f_7 = tripletFields.srcAttr("7")
                    f_7 = f_7 - omega*(f_7 - f_7_eq)

                    tripletFields.sendToDst(Map(
                        "1" -> tripletFields.dstAttr("1"),
                        "2" -> tripletFields.dstAttr("2"),
                        "3" -> tripletFields.dstAttr("3"),
                        "4" -> tripletFields.dstAttr("4"),
                        "5" -> tripletFields.dstAttr("5"),
                        "6" -> tripletFields.dstAttr("6"),
                        "7" -> f_7,
                        "8" -> tripletFields.dstAttr("8"),
                        "0" -> tripletFields.dstAttr("0"),
                        "ux" -> tripletFields.dstAttr("ux"),
                        "uy" -> tripletFields.dstAttr("uy")
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)


         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[String,Double]](tripletFields => { 
            if(tripletFields.attr == "8"){
                    val rho: Double = tripletFields.srcAttr("1") +
                      tripletFields.srcAttr("2") +
                      tripletFields.srcAttr("3") +
                      tripletFields.srcAttr("4") +
                      tripletFields.srcAttr("5") +
                      tripletFields.srcAttr("6") + 
                      tripletFields.srcAttr("7") +
                      tripletFields.srcAttr("8") +
                      tripletFields.srcAttr("0")
                    val ux: Double = tripletFields.srcAttr("ux")
                    val uy: Double = tripletFields.srcAttr("uy")
                    val c: Double = 1/scala.math.sqrt(3)
                    val c8: Array[Double] = Array(-c, -c)
                    val c_square: Double = c*c 
                    val omega: Double =  1.98
                    val epsilon_8: Double = 1/36
                    val term_1: Double = (c8(0)*uy + c8(1)*ux)/c_square
                    val term_2: Double = ((c8(0)*uy + c8(1)*ux)*(c8(0)*uy + c8(1)*ux) - (uy*uy + ux*ux)*(uy*uy + ux*ux)*c_square)/(2*c_square*c_square)
                    val f_8_eq: Double = rho*epsilon_8*(1 + term_1 + term_2)
                    var f_8 = tripletFields.srcAttr("8")
                    f_8 = f_8 - omega*(f_8 - f_8_eq)

                    tripletFields.sendToDst(Map(
                        "1" -> tripletFields.dstAttr("1"),
                        "2" -> tripletFields.dstAttr("2"),
                        "3" -> tripletFields.dstAttr("3"),
                        "4" -> tripletFields.dstAttr("4"),
                        "5" -> tripletFields.dstAttr("5"),
                        "6" -> tripletFields.dstAttr("6"),
                        "7" -> tripletFields.dstAttr("7"),
                        "8" -> f_8,
                        "0" -> tripletFields.dstAttr("0"),
                        "ux" -> tripletFields.dstAttr("ux"),
                        "uy" -> tripletFields.dstAttr("uy")
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

        // Calculating Velocities
        cellsStreamed = 
         cellsStreamed.map(cell => {  
            /*
                8  1  2
                7  0  3
                6  5  4
            */

            val c = 1/scala.math.sqrt(3)
            val ci: Map[String,Array[Double]] = Map("c0" -> Array( 0, 0),
                         "c1" -> Array(+c, 0),
                         "c2" -> Array(+c,+c),
                         "c3" -> Array( 0,+c),
                         "c4" -> Array(-c,+c),
                         "c5" -> Array(-c, 0),
                         "c6" -> Array(-c,-c),
                         "c7" -> Array( 0,-c),
                         "c8" -> Array(c,-c))
            val rho: Double = cell._2("1") +
                      cell._2("2") +
                      cell._2("3") +
                      cell._2("4") +
                      cell._2("5") +
                      cell._2("6") + 
                      cell._2("7") +
                      cell._2("8") +
                      cell._2("0")
            val ux: Double = (cell._2("1")*ci("c1")(1) +
                     cell._2("2")*ci("c2")(1) +
                     cell._2("3")*ci("c3")(1) +
                     cell._2("4")*ci("c4")(1) +
                     cell._2("5")*ci("c5")(1) +
                     cell._2("6")*ci("c6")(1) + 
                     cell._2("7")*ci("c7")(1) +
                     cell._2("8")*ci("c8")(1) +
                     cell._2("0")*ci("c0")(1))/rho
            val uy: Double = (cell._2("1")*ci("c1")(0) +
                     cell._2("2")*ci("c2")(0) +
                     cell._2("3")*ci("c3")(0) +
                     cell._2("4")*ci("c4")(0) +
                     cell._2("5")*ci("c5")(0) +
                     cell._2("6")*ci("c6")(0) + 
                     cell._2("7")*ci("c7")(0) +
                     cell._2("8")*ci("c8")(0) +
                     cell._2("0")*ci("c0")(0))/rho

            (cell._1, Map("1" -> cell._2("1"),
                "2" -> cell._2("2"),
                "3" -> cell._2("3"),
                "4" -> cell._2("4"),
                "5" -> cell._2("5"),
                "6" -> cell._2("6"),
                "7" -> cell._2("7"),
                "8" -> cell._2("8"),
                "0" -> cell._2("0"),
                "ux" -> ux,
                "uy" -> uy  ))
        })
        latticeBefore = Graph(cellsStreamed, relationshipCells)

    }

    println("Resposta =======================")
    val cellsResultAfter = latticeBefore.mapVertices((id, attr) => attr("1") +
    attr("2") + attr("3")
   + attr("4")
   + attr("5")
   + attr("6")
   + attr("7")
   + attr("8") 
   + attr("0"))
    //val cellsResultAfter = latticeBefore.mapVertices((id, attr) => attr("ux"))
    cellsResultAfter.vertices.saveAsTextFile("After.txt")
    println("Resposta =======================")

    // do stuff
    println("************")
    println("************")
    println("Mone delicia!")
    println("************")
    println("************")
    
    // terminate spark context
    sc.stop()
    
  }
}

