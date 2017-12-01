// import required spark classes
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
 
// define main method (Spark entry point)
object HelloWorld {
  def main(args: Array[String]) {
 
    // initialise spark context
    val conf = new SparkConf().setAppName("HelloWorld")
    val sc = new SparkContext(conf)

    /*val cells = sc.parallelize(Array((1L,Array(0, 0.1, 0,0,0,0,0,0,0)),
        (2L,Array(0, 0.2,0,0,0,0,0,0,0)),
        (3L,Array(0, 0.3,0,0,0,0,0,0,0)),
        (4L,Array(0, 0.4,0,0,0,0,0,0,0)),
        (5L,Array(0, 0.5,0,0,0,0,0,0,0)),
        (6L,Array(0, 0.6,0,0,0,0,0,0,0)),
        (7L,Array(0, 0.7,0,0,0,0,0,0,0)),
        (8L,Array(0, 0.8,0,0,0,0,0,0,0)),
        (9L,Array(0, 0.9,0,0,0,0,0,0,0))))*/

    val cells = sc.parallelize(Array((1L,Map('1' -> 0.1, '3' -> 1.0, '2' -> 10.0, '4' -> 100.0, '5' -> 1000.0, '6' -> 10000.0, '7' -> 100000.0, '8' -> 1000000.0, '0' -> 0.0)),
        (2L,Map('1' -> 0.2, '3' -> 2.0, '2' -> 20.0, '4' -> 200.0, '5' -> 2000.0, '6' -> 20000.0, '7' -> 200000.0, '8' -> 2000000.0, '0' -> 0.0)),
        (3L,Map('1' -> 0.3, '3' -> 3.0, '2' -> 30.0, '4' -> 300.0, '5' -> 3000.0, '6' -> 30000.0, '7' -> 300000.0, '8' -> 3000000.0, '0' -> 0.0)),
        (4L,Map('1' -> 0.4, '3' -> 4.0, '2' -> 40.0, '4' -> 400.0, '5' -> 4000.0, '6' -> 40000.0, '7' -> 400000.0, '8' -> 4000000.0, '0' -> 0.0)),
        (5L,Map('1' -> 0.5, '3' -> 5.0, '2' -> 50.0, '4' -> 500.0, '5' -> 5000.0, '6' -> 50000.0, '7' -> 500000.0, '8' -> 5000000.0, '0' -> 0.0)),
        (6L,Map('1' -> 0.6, '3' -> 6.0, '2' -> 60.0, '4' -> 600.0, '5' -> 6000.0, '6' -> 60000.0, '7' -> 600000.0, '8' -> 6000000.0, '0' -> 0.0)),
        (7L,Map('1' -> 0.7, '3' -> 7.0, '2' -> 70.0, '4' -> 700.0, '5' -> 7000.0, '6' -> 70000.0, '7' -> 700000.0, '8' -> 7000000.0, '0' -> 0.0)),
        (8L,Map('1' -> 0.8, '3' -> 8.0, '2' -> 80.0, '4' -> 800.0, '5' -> 8000.0, '6' -> 80000.0, '7' -> 800000.0, '8' -> 8000000.0, '0' -> 0.0)),
        (9L,Map('1' -> 0.9, '3' -> 9.0, '2' -> 90.0, '4' -> 900.0, '5' -> 900.0, '6' -> 9000.0, '7' -> 90000.0, '8' -> 900000.0, '0' -> 0.0))))

/*val cells = sc.parallelize(Array((1L,0.1),
        (2L,0.2),
        (3L,0.3),
        (4L,0.4),
        (5L,0.5),
        (6L,0.6),
        (7L,0.7),
        (8L,0.8),
        (9L,0.9)))*/

    val relationshipCells = sc.parallelize(Array(Edge(1L,7L,'1'),
        Edge(1L,8L,'2'),
        Edge(1L,2L,'3'),
        Edge(1L,5L,'4'),
        Edge(1L,4L,'5'),
        Edge(1L,6L,'6'),
        Edge(1L,3L,'7'),
        Edge(1L,9L,'8'),
        Edge(3L,9L,'1'),
        Edge(3L,7L,'2'),
        Edge(3L,1L,'3'),
        Edge(3L,4L,'4'),
        Edge(3L,6L,'5'),
        Edge(3L,5L,'6'),
        Edge(3L,2L,'7'),
        Edge(3L,8L,'8'),
        Edge(9L,6L,'1'),
        Edge(9L,4L,'2'),
        Edge(9L,7L,'3'),
        Edge(9L,1L,'4'),
        Edge(9L,3L,'5'),
        Edge(9L,2L,'6'),
        Edge(9L,8L,'7'),
        Edge(9L,5L,'8'),
        Edge(7L,4L,'1'),
        Edge(7L,5L,'2'),
        Edge(7L,8L,'3'),
        Edge(7L,2L,'4'),
        Edge(7L,1L,'5'),
        Edge(7L,3L,'6'),
        Edge(7L,9L,'7'),
        Edge(7L,6L,'8'),
        Edge(2L,8L,'1'),
        Edge(2L,9L,'2'),
        Edge(2L,3L,'3'),
        Edge(2L,6L,'4'),
        Edge(2L,5L,'5'),
        Edge(2L,4L,'6'),
        Edge(2L,1L,'7'),
        Edge(2L,7L,'8'),
        Edge(6L,3L,'1'),
        Edge(6L,1L,'2'),
        Edge(6L,4L,'3'),
        Edge(6L,7L,'4'),
        Edge(6L,9L,'5'),
        Edge(6L,8L,'6'),
        Edge(6L,5L,'7'),
        Edge(6L,2L,'8'),
        Edge(8L,5L,'1'),
        Edge(8L,6L,'2'),
        Edge(8L,9L,'3'),
        Edge(8L,3L,'4'),
        Edge(8L,2L,'5'),
        Edge(8L,1L,'6'),
        Edge(8L,7L,'7'),
        Edge(8L,4L,'8'),
        Edge(4L,1L,'1'),
        Edge(4L,2L,'2'),
        Edge(4L,5L,'3'),
        Edge(4L,8L,'4'),
        Edge(4L,7L,'5'),
        Edge(4L,9L,'6'),
        Edge(4L,6L,'7'),
        Edge(4L,3L,'8'),
        Edge(5L,2L,'1'),
        Edge(5L,3L,'2'),
        Edge(5L,6L,'3'),
        Edge(5L,9L,'4'),
        Edge(5L,8L,'5'),
        Edge(5L,7L,'6'),
        Edge(5L,4L,'7'),
        Edge(5L,1L,'8')))

    var latticeBefore = Graph(cells, relationshipCells)

    println("Resposta =======================")
    val cellsResultBefore = latticeBefore.mapVertices((id, attr) =>  (id, attr('1').toString + " - " + attr('3').toString ))
    cellsResultBefore.vertices.saveAsTextFile("Before.txt")
    println("Resposta =======================")


    var cellsStreamed = cells
    //for( a <- 1 to 2){
      

        cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if (tripletFields.attr == '1'){
                tripletFields.sendToDst(Map(
                    '1' -> tripletFields.srcAttr('1'),
                    '2' -> tripletFields.dstAttr('2'),
                    '3' -> tripletFields.dstAttr('3'),
                    '4' -> tripletFields.dstAttr('4'),
                    '5' -> tripletFields.dstAttr('5'),
                    '6' -> tripletFields.dstAttr('6'),
                    '7' -> tripletFields.dstAttr('7'),
                    '8' -> tripletFields.dstAttr('8'),
                    '0' -> tripletFields.dstAttr('0')
                ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

        cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if (tripletFields.attr == '2'){
                tripletFields.sendToDst(Map(
                    '1' -> tripletFields.dstAttr('1'),
                    '2' -> tripletFields.srcAttr('2'),
                    '3' -> tripletFields.dstAttr('3'),
                    '4' -> tripletFields.dstAttr('4'),
                    '5' -> tripletFields.dstAttr('5'),
                    '6' -> tripletFields.dstAttr('6'),
                    '7' -> tripletFields.dstAttr('7'),
                    '8' -> tripletFields.dstAttr('8'),
                    '0' -> tripletFields.dstAttr('0')
                ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)   

        cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if(tripletFields.attr == '3'){
                    tripletFields.sendToDst(Map(
                        '1' -> tripletFields.dstAttr('1'),
                        '2' -> tripletFields.dstAttr('2'),
                        '3' -> tripletFields.srcAttr('3'),
                        '4' -> tripletFields.dstAttr('4'),
                        '5' -> tripletFields.dstAttr('5'),
                        '6' -> tripletFields.dstAttr('6'),
                        '7' -> tripletFields.dstAttr('7'),
                        '8' -> tripletFields.dstAttr('8'),
                        '0' -> tripletFields.dstAttr('0')
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if(tripletFields.attr == '4'){
                    tripletFields.sendToDst(Map(
                        '1' -> tripletFields.dstAttr('1'),
                        '2' -> tripletFields.dstAttr('2'),
                        '3' -> tripletFields.dstAttr('3'),
                        '4' -> tripletFields.srcAttr('4'),
                        '5' -> tripletFields.dstAttr('5'),
                        '6' -> tripletFields.dstAttr('6'),
                        '7' -> tripletFields.dstAttr('7'),
                        '8' -> tripletFields.dstAttr('8'),
                        '0' -> tripletFields.dstAttr('0')
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if(tripletFields.attr == '5'){
                    tripletFields.sendToDst(Map(
                        '1' -> tripletFields.dstAttr('1'),
                        '2' -> tripletFields.dstAttr('2'),
                        '3' -> tripletFields.dstAttr('3'),
                        '4' -> tripletFields.dstAttr('4'),
                        '5' -> tripletFields.srcAttr('5'),
                        '6' -> tripletFields.dstAttr('6'),
                        '7' -> tripletFields.dstAttr('7'),
                        '8' -> tripletFields.dstAttr('8'),
                        '0' -> tripletFields.dstAttr('0')
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if(tripletFields.attr == '6'){
                    tripletFields.sendToDst(Map(
                        '1' -> tripletFields.dstAttr('1'),
                        '2' -> tripletFields.dstAttr('2'),
                        '3' -> tripletFields.dstAttr('3'),
                        '4' -> tripletFields.dstAttr('4'),
                        '5' -> tripletFields.dstAttr('5'),
                        '6' -> tripletFields.srcAttr('6'),
                        '7' -> tripletFields.dstAttr('7'),
                        '8' -> tripletFields.dstAttr('8'),
                        '0' -> tripletFields.dstAttr('0')
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)


         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if(tripletFields.attr == '7'){
                    tripletFields.sendToDst(Map(
                        '1' -> tripletFields.dstAttr('1'),
                        '2' -> tripletFields.dstAttr('2'),
                        '3' -> tripletFields.dstAttr('3'),
                        '4' -> tripletFields.dstAttr('4'),
                        '5' -> tripletFields.dstAttr('5'),
                        '6' -> tripletFields.dstAttr('6'),
                        '7' -> tripletFields.srcAttr('7'),
                        '8' -> tripletFields.dstAttr('8'),
                        '0' -> tripletFields.dstAttr('0')
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)


         cellsStreamed = 
        latticeBefore.aggregateMessages[Map[Char,Double]](tripletFields => { 
            if(tripletFields.attr == '8'){
                    tripletFields.sendToDst(Map(
                        '1' -> tripletFields.dstAttr('1'),
                        '2' -> tripletFields.dstAttr('2'),
                        '3' -> tripletFields.dstAttr('3'),
                        '4' -> tripletFields.dstAttr('4'),
                        '5' -> tripletFields.dstAttr('5'),
                        '6' -> tripletFields.dstAttr('6'),
                        '7' -> tripletFields.dstAttr('7'),
                        '8' -> tripletFields.srcAttr('8'),
                        '0' -> tripletFields.dstAttr('0')
                    ))
            }
        },
        (a, b) => (a))
        latticeBefore = Graph(cellsStreamed, relationshipCells)

    //}

    /*val cellsStreamed = 
    latticeBefore.aggregateMessages[Array[Double]](tripletFields => { 
        if (tripletFields.attr == '1'){
            var arrayDensity = tripletFields.dstAttr
            arrayDensity(1) = tripletFields.srcAttr(1)
            tripletFields.sendToDst(arrayDensity)
        }
        if (tripletFields.attr == '2'){
            var arrayDensity = tripletFields.dstAttr
            arrayDensity(2) = tripletFields.srcAttr(2)
            tripletFields.sendToDst(arrayDensity)
        }

    },
    (a, b) => (a))

    val latticeAfter = Graph(cellsStreamed, relationshipCells)*/

    println("Resposta =======================")
    val cellsResultAfter = latticeBefore.mapVertices((id, attr) => attr('1').toString + " " +
    attr('2').toString + " " + attr('3').toString +
     " " + attr('4').toString +
     " " + attr('5').toString +
     " " + attr('6').toString +
     " " + attr('7').toString +
     " " + attr('8').toString + 
     " " + attr('0').toString)
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

