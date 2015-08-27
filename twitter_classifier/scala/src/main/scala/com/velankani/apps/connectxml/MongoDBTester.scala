package com.velankani.apps.connectxml
import com.mongodb.casbah.Imports._

/**
 * Object to test if mongodb is up, just inserts something simple and then
 * user can go check mongo to see if it's there.
 */
object MongoDBTester {
	def main(args: Array[String]): Unit = {
	  val mongoClient = MongoClient(sys.env("MONGO_IP"), sys.env("MONGO_PORT").toInt)
	  val db = mongoClient("test")
	  val coll = db("test")
	  val a = MongoDBObject("hi" -> "bye")
	  coll.insert(a)
	}
	
}
