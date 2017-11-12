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

    val cells = sc.parallelize(Array((100L, Array("A")), 
        (200L, Array("B")), 
        (300L, Array("C")), 
        (400L, Array("D")), 
        (500L, Array("E")), 
        (600L, Array("F")), 
        (700L, Array("G")), 
        (800L, Array("H")), 
        (900L, Array("I"))))

    //100 200 300 400 500 600 700 800 900
    val relationshipCells = sc.parallelize(Array(Edge(100L, 200L,"left"), 
        Edge(200L, 300L,"left"), 
        Edge(300L, 400L,"left"),
        Edge(400L, 500L,"left"), 
        Edge(500L, 600L,"left"), 
        Edge(600L, 700L,"left"), 
        Edge(700L, 800L,"left"),
        Edge(800L, 900L,"left"),
        Edge(900L, 100L,"left")))    
    
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

