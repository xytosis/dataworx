package com.velankani.apps.connectxml

import com.mongodb.casbah.Imports._

import org.apache.spark.{SparkConf, SparkContext}

import org.anormcypher._

import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

import scala.collection.JavaConversions._
import scala.util.parsing.json._


/**
 * This is a batch job that should be automated by cron to run every 15-20 minutes and checks 
 * for users that have not been checked for being a bot. It checks up to 150 users timelines at
 * once, and is limited to this because of the twitter api search rate limit.
 */
class TwitterTimelineCheck(key: String, secret: String, access: String, accessSecret: String) {
    println("Initializing Neo4j connector")
    implicit val connection = Neo4jREST(sys.env("NEO_IP"), sys.env("NEO_PORT").toInt, sys.env("NEO_DB"),
        sys.env("NEO_USER"), sys.env("NEO_PASSWORD"))

    println("Initializing Mongo connector")
    val mongoClient = MongoClient(sys.env("MONGO_IP"), sys.env("MONGO_PORT").toInt)
    val db = mongoClient("test")
    val coll = db("test")

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true).setOAuthConsumerKey(key).setOAuthConsumerSecret(secret).setOAuthAccessToken(access).setOAuthAccessTokenSecret(accessSecret)
    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()

    def run(): Unit = {
        val results = Cypher("MATCH (p:PERSON) WHERE NOT HAS (p.is_bot) return p.name as name, p.id as id LIMIT 150")
        val idToName = results.apply().map(row => row[String]("id") -> row[String]("name")).toList
        println("Preparing to check timelimes")
        val idnameToRating = idToName.map{x =>
            val res = timelineCheck(x._2)
            (x, res)
        }
        println("Preparing to update DBs")
        idnameToRating.map{x =>
            updateNeo(x)
            updateMongo(x)
        }
        println("finished updating")
    }

    /**
     * Update the neo4j nodes with their bot information
     */
    def updateNeo(tup: ((String, String), (Int, Int, Int, Boolean))) = {
        Cypher("MATCH (p:PERSON {id:\"" + tup._1._1 + "\"}) "
            + "SET p += {avg_tweet_freq:\"" + tup._2._1 + "\""
            + ", avg_tweet_reg:\"" + tup._2._2 + "\""
            + ", tweet_freq_variance:\"" + tup._2._3 + "\""
            + ", is_bot:" + tup._2._4 + "}").execute()
    }

    /**
     * Updates the mongodb nodes with their bot information
     * Inserts the data into a metadata section of the document
     */
    def updateMongo(tup: ((String, String), (Int, Int, Int, Boolean))) = {
        val query = DBObject("user.id"-> tup._1._1)
        val update = $set("metadata" -> DBObject("avg_tweet_freq" -> tup._2._1,
            "avg_tweet_reg" -> tup._2._2, "tweet_freq_variance" -> tup._2._3,
            "is_bot" -> tup._2._4))
        coll.update(query, update, multi=true)
    }

    /**
     * Looks up the user's timeline and checks the spacings between tweets
     * to see if it might be a bot or something. Might be bot if tweets are
     * evenly spaced (once every 10 min) or if many tweets in sort amount of
     * time (RecrewtCo).
     */
    def timelineCheck(user: String): (Int, Int, Int, Boolean) = {
        var tooFast = 0
        var tooRegular = 0
        var variance = 0
        var isBot = false
        val timeline = twitter.getUserTimeline(user)
        // get the time each tweet was created at
        val times = timeline.map(_.getCreatedAt).toArray
        // get the seconds between each tweet
        var timediff: List[Int] = List()
        for (i <- 1 until times.length) {
            timediff = timediff :+ ((times(i-1).getTime - times(i).getTime) / 1000).toInt
        }
        // now have to see if the tweets happen too frequently or even intervals
        var timediffdiff: List[Int] = List()
        for (i <- 1 until timediff.length) {
            timediffdiff = timediffdiff :+ (timediff(i-1) - timediff(i))
        }
    	var timedifflength = timediff.length
    	var timediffdifflength = timediffdiff.length
        // make sure you don't divide by 0 accidentally
    	if (timedifflength == 0) timedifflength = 1
    	if (timediffdifflength == 0) timediffdifflength = 1
        tooFast = timediff.sum / timedifflength
        tooRegular = timediffdiff.sum / timediffdifflength
        variance = timediff.map(x => (x - tooFast) * (x - tooFast)).sum
        // if account tweets too fast or too regularly, it is probably a bot
        if (tooFast < 600 || tooRegular < 60 && tooRegular > -60) isBot = true
        // if variance is high enough, then the averages may have been a coincidence
        if (variance > 500000000) isBot = false
        return (tooFast, tooRegular, variance, isBot)
    }
}

object TwitterTimelineCheck {
    def main(args: Array[String]): Unit = {
        if (args.length != 4) {
            System.err.println("Usage: " + this.getClass.getSimpleName + " <key>  <secret> <access> <accessSecret>")
            println(args.toList)
            System.exit(1)
        }
        val t = new TwitterTimelineCheck(args(0), args(1), args(2), args(3))
        t.run
        System.exit(0)
    }
}
