package com.velankani.apps.connectxml

import org.anormcypher._

/**
 * Object that does a simple test to see if the neo4j database is working.
 * Writes a test object to the db and then queries it and prints it.
 */
object Neo4jTester {
	def main(args: Array[String]): Unit = {
	  implicit val connection = Neo4jREST(sys.env("NEO_IP"), sys.env("NEO_PORT").toInt, sys.env("NEO_DB"),
	  	sys.env("NEO_USER"), sys.env("NEO_PASSWORD"))
	  Cypher("""create (anorm {name:"AnormCypher"}), (test {name:"Test"})""").execute()
	  val req = Cypher("start n=node(*) return n.name")
	  val stream = req()
	  println(stream.map(row => {row[String]("n.name")}).toList)

	  sys.exit(0)
	}
	
}
