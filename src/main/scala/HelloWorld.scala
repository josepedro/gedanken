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

    var cells_info = Array("A", "B", "C", "D", "E", "F", "G", "H", "I")

    val cells = sc.parallelize(Array((1L, cells_info), 
        (2L, cells_info), 
        (3L, cells_info), 
        (4L, cells_info), 
        (5L, cells_info), 
        (6L, cells_info), 
        (7L, cells_info), 
        (8L, cells_info), 
        (9L, cells_info)))

    //1 2 3 4 5 6 7 8 9
    val relationshipCells = sc.parallelize(Array(Edge(5L, 2L,"1"), 
        Edge(2L, 3L,"left"), 
        Edge(3L, 4L,"left"),
        Edge(4L, 5L,"left"), 
        Edge(5L, 6L,"left"), 
        Edge(6L, 7L,"left"), 
        Edge(7L, 8L,"left"),
        Edge(8L, 9L,"left"),
        Edge(9L, 1L,"left")


        ))    
    
    val latticeBefore = Graph(cells, relationshipCells)

    println("Resposta =======================")
    val cellsResultBefore = latticeBefore.mapVertices((id, attr) =>  (id, attr(0)))
    cellsResultBefore.vertices.saveAsTextFile("Before.txt")
    println("Resposta =======================")

    val cellsStreamed = 
    latticeBefore.aggregateMessages[Array[String]](tripletFields => { 
        val density = tripletFields.srcAttr
        if (tripletFields.attr == "left"){tripletFields.sendToDst(density)}
        else {}
    },
    (a, b) => (a))

    val latticeAfter = Graph(cellsStreamed, relationshipCells)

    println("Resposta =======================")
    val cellsResultAfter = latticeAfter.mapVertices((id, attr) =>  (id, attr(0)))
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

