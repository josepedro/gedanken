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

    val cells = sc.parallelize(Array((1L,Array(0, 0.1, 1,0,0,0,0,0,0)),
        (2L,Array(0, 0.2,2,0,0,0,0,0,0)),
        (3L,Array(0, 0.3,3,0,0,0,0,0,0)),
        (4L,Array(0, 0.4,4,0,0,0,0,0,0)),
        (5L,Array(0, 0.5,5,0,0,0,0,0,0)),
        (6L,Array(0, 0.6,6,0,0,0,0,0,0)),
        (7L,Array(0, 0.7,7,0,0,0,0,0,0)),
        (8L,Array(0, 0.8,8,0,0,0,0,0,0)),
        (9L,Array(0, 0.9,9,0,0,0,0,0,0))))

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
    val cellsResultBefore = latticeBefore.mapVertices((id, attr) =>  (id, attr(1) + attr(2) ))
    cellsResultBefore.vertices.saveAsTextFile("Before.txt")
    println("Resposta =======================")


    var cellsStreamed = cells
    for( a <- 1 to 2){
        cellsStreamed = 
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

        latticeBefore = Graph(cellsStreamed, relationshipCells)     
    }

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
    val cellsResultAfter = latticeBefore.mapVertices((id, attr) =>  (id, attr(1) + attr(2) ))
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

