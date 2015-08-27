package com.velankani.apps.connectxml
import com.mongodb.casbah.Imports._

import kafka.serializer.StringDecoder

import java.security.MessageDigest

import org.anormcypher._

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF

import java.io.FileInputStream
import org.apache.hadoop.fs.FSDataInputStream
import java.io.ObjectInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import twitter4j._

/**
 * Pulls live tweets and filters them for tweets in the chosen cluster.
 */
object NaiveBayesPredict {

  // for deserializing tweets from kafka
  val encode: BASE64Encoder = new BASE64Encoder()
  val decode: BASE64Decoder = new BASE64Decoder()

  println("Initializing MongoDB connector")
  val mongoClient = MongoClient(sys.env("MONGO_IP"), sys.env("MONGO_PORT").toInt)
  val db = mongoClient("test")
  val coll = db("test")

  println("Initializing Neo4j connector")
  // put password and username and db here
  implicit val connection = Neo4jREST(sys.env("NEO_IP"), sys.env("NEO_PORT").toInt, sys.env("NEO_DB"),
    sys.env("NEO_USER"), sys.env("NEO_PASSWORD"))

  // List of sites that are job aggregators
  var jobsites: Set[String] = Set("adzuna","aftercollege","akhtaboot","aljazeerajobs",
    "americasjobexchange","babajob","jobbank","careerbuilder","careerstructure","coolavenues",
    "dice","eluta","employiq","entertainmentcareers","fins","glassdoor","gumtree",
    "hispanicbusiness","hound","iaeste","igrad","indeed","insidetrak","internships",
    "internshala","ivyexec","jobandtalent","jobberman","jobbi","jobindex","jobing",
    "jobs.ac.uk","jobserve","jobstreet","jobtrak","kijiji","lawcrossing","linkedin",
    "linkup","moster","myscience","maukri","navent","neuvoo","oodle","peopleperhour",
    "proven","reed.co.uk","researchgate","rndeer","simplyhired","smartmatch","snagajob",
    "spotjobs","studentgems","swissnex","talentzoo","talentegg","employmentguide","theladders",
    "themuse","timesjobs","tolmol","trovit","tweetmyjobs","uloop","usajobs","workopolis",
    "workpop","ziprecruiter")

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: " + this.getClass.getSimpleName
        + " <modelDirectory>  <clusterNumber> <kafkabrokerlist> <kafkatopics>")
      System.exit(1)
    }

    val Array(modelFile:String, tfidfFile:String, brokers:String, topics:String) =
      Utils.parseCommandLineWithTwitterCredentials(args)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    println("Making tfidf word vectorizer")
    val documents = sc.textFile(tfidfFile)
    // ratings are the 1/0 values to say whether it's a job or not
    val ratings = documents.map(_.split("\\|#\\|")(5))
    // transforms the documents to a sequence of strings
    val docsToSeq = documents.map(_.split("\\|#\\|")(4)
        .replaceAll("""http:\/\/[a-zA-Z\/.0-9]*""", "")
        .replaceAll("""[\p{Punct}&&[^.#]]""", "")
        .split("\\s").map(x => PorterStemmer.stem(x)).mkString(" ").toSeq)
    val hashingTF = new HashingTF()
    // term frequency vectors for each doc
    val tf = hashingTF.transform(docsToSeq)

    tf.cache()

    val idf = new IDF().fit(tf)

    println("Initalizaing the the NaiveBayes model...")
     val model = NaiveBayesModel.load(sc, modelFile)

    println("Initializing web scraper")
    val scraper = new ScalaScraper("http://www.example.com/")


    println("Initializing Streaming Spark Context...")
    val ssc = new StreamingContext(sc, Seconds(1))

    println("Initializing Twitter stream...")
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val tweets = messages.map(x => SToO(x._2).asInstanceOf[Status])
    val statuses = tweets.transform { rdd => {
        rdd.filter(x => x.getLang() == "en" && x.getText.length > 80)
      }
    }
    // After deserializing the tweets from kafka and making sure they are english
    // only include the tweets with job related words in them, then run ml prediction
    val filteredTweets = statuses.transform { rdd =>rdd.map{t => 
      val temp = t.getText.toLowerCase()
      if (temp.contains("job") || temp.contains("java")
        || temp.contains("php") || temp.contains("python") || temp.contains("work")
        || temp.contains("apply") || temp.contains("career") || temp.contains("hire")
        || temp.contains("hiring")) t else null}
      .map(t => if (t != null) (model.predict(idf.transform(hashingTF.transform(t.getText.replaceAll("""http:\/\/[a-zA-Z\/.0-9]*""", "")
        .replaceAll("""[\p{Punct}&&[^#]]""", "")
        .split("\\s").map(x => PorterStemmer.stem(x)).toSeq))), t) else (0.0, t))
    }
    // For every tweet that is a job tweet, run web scraper and then add to dbs
    val jobTweets = filteredTweets.transform {rdd => rdd.map{x => if(x._1 == 1.0) 
          {
            if (checkTweetExists(x._2.getText)) {
              "tweet already exists"
            } else {
              // get the link
              val links = getLinks(x._2.getText)
              var phone: String = "[]"
              var email: String = "[]"
              // commented out for now because of unknown host exception t.co
              //if (links.length > 0) {
              //    scraper.changeUrl(links.head)
              //    phone = listToJSON(scraper.getPhones())
              //    email = listToJSON(scraper.getEmails())
              //}
              addToMongo(x._2, phone, email)
              val r = addToNeo(x._2, phone, email)
              "should have inserted something | " + r
              }
          }
        }
    }
    jobTweets.print()
    // Start the streaming computation
    println("Initialization complete.")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Takes in the text of a tweet and extract any links that it can find within it.
   * Returns a list of urls
   */
  def getLinks(text: String): List[String] = {
    var toReturn: List[String] = List()
    val re = """\b((?:[a-z][\w-]+:(?:\/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))""".r
    for (m <- re.findAllIn(text)) {
      toReturn = toReturn :+ m
    }
    return toReturn
  }

  /**
  * Turns tweet into a map and then inserts that into mongodb
  */
  def addToMongo(status: Status, phone: String, email: String): Unit = {
    var a = statusToMap(status).asDBObject
    // insert phone and email into map
    var link = ""
    try { 
      link = getLinks(status.getText).head
    } catch {
      case e: Exception => link = ""
    }
    val third_party = checkIfAggregator(status)
    a += "user" -> userToMap(status.getUser.toString).asDBObject
    a += "phone" -> phone
    a += "email" -> email
    a += "link" -> link
    a += "hash" -> md5(status.getText)
    a += "createdAt" -> status.getCreatedAt
    coll.insert(a)
  }

  /**
  * Turns a twitter4j status into a simple map
  */
  def statusToMap(status: Status): Map[String, String] = {
    var place = ""
    try { 
      place = status.getPlace.toString
    } catch {
      case e: Exception => place = ""
    }
    if (status.isRetweet) {
      return Map("source" -> status.getSource,
      "tweetId" -> status.getRetweetedStatus.getId.toString, "text" -> status.getText,
      "favorites" -> status.getFavoriteCount.toString, "place" -> place)
    }
    return Map("source" -> status.getSource,
      "tweetId" -> status.getId.toString, "text" -> status.getText,
      "favorites" -> status.getFavoriteCount.toString, "place" -> place)
  }


  /**
  * Tries to parse the user string from twitter4j, returns a map
  */
  def userToMap(user: String): Map[String, String] = {
    val toSplit: String = user.replaceAll("UserJSONImpl\\{", "").replaceAll("\\}", "")
    val kvPairs: Array[String] = toSplit.split(",(?=([^\']*\'[^\']*\')*[^\']*$)", -1).map(_.trim())
    
    try { 
      return kvPairs.map{x =>
           val y = x.split("=")
           (y(0), y(1).replaceAll("'", ""))}.toMap
    } catch {
      case e: Exception => println(user)
      return null
    }
  }

  /**
  * Gets all the hashtags in a tweet and puts them in an array
  */
  def getHashtags(tweet: String): List[String] = {
    var toReturn: List[String] = List()
    val re = "(#\\w+)".r
    for(m <- re.findAllIn(tweet)) {
      toReturn = toReturn :+ m.replaceAll("#", "").toLowerCase
    }
    return toReturn
  }

  /**
   * Turns a list into its json representation
   */
  def listToJSON(l :List[Any]): String = {
    var toReturn = "["
    for (i <- l) {
      toReturn += i.toString + ","
    }
    if (toReturn.endsWith(",")) return toReturn.substring(0, toReturn.length - 1) + "]"
    return "[]"
  }

  /**
  * Adds a tweet and related people to the neo4j database. Adds a person -> tweet relationship
  * and also adds a tweet->tag relationship. This makes finding tweets by people or tags much
  * faster.
  */
  def addToNeo(status: Status, phone: String, email: String): String = {
    val statusMap: Map[String, String] = statusToMap(status)
    val userMap: Map[String, String] = userToMap(status.getUser.toString)
    var relationship = "TWEETED"
    if (status.isRetweet) {
      relationship = "RETWEETED"
    }
    var link = ""
    try { 
      link = getLinks(status.getText).head
    } catch {
      case e: Exception => link = ""
    }
    val third_party = checkIfAggregator(status)
    // Insert the person node into neo4j
    val result1: Boolean = Cypher("""MERGE (p:PERSON {id:"""
      + "\"" + userMap("id") + "\"" + """, name: """
      + "\"" + userMap("screenName") + "\"" + """ })""").execute()
    // insert the tweet node into neo4j
    val result2: Boolean = Cypher("""MERGE (t:TWEET {id:"""
      + "\"" + statusMap("tweetId") + "\"" + """, text: """
      + "\"" + statusMap("text") + "\"" + """, createdAt:""" + "\"" + status.getCreatedAt.getTime
      + "\"" + """, tags:""" + "\"" + getHashtags(statusMap("text")) + "\""
      + """, phone:""" + "\"" + phone + "\""
      + """, email:""" + "\"" + email + "\""
      + """, link:""" + "\"" + link + "\""
      + """, hash:""" + "\"" + md5(statusMap("text")) + "\""
      + """, third_party:""" + "\"" + third_party + "\""
      + """ })""").execute()
    // create relationship between person and tweet
    val result3: Boolean = Cypher("""MATCH  (p:PERSON {id:"""
      + "\"" + userMap("id") + "\"" + """, name: """
      + "\"" + userMap("screenName") + "\"" + """ }), (t:TWEET {id:"""
      + "\"" + statusMap("tweetId") + "\"" + """ })"""
      + """ CREATE UNIQUE (p) -[:""" + relationship + """]-> (t)""").execute()
    // create and make relationship between tag nodes and tweets
    var boolList :List[Boolean] = List()
    for (tag <- getHashtags(statusMap("text"))) {
      boolList = boolList :+ Cypher("MERGE (tag:TAG {name:\"" + tag + "\"})").execute()
      boolList = boolList :+ Cypher("MATCH (t:TWEET {id:\""
        + statusMap("tweetId") + "\"}), (tag:TAG {name:\"" + tag + "\"}) CREATE UNIQUE (t)-[:TAGGED]->(tag)").execute()
    }
    // make sure all tags were inserted correctly
    val result4 = boolList.fold(true) { (a, i) =>
      a && i
    }
    return "" + result1 + " " + result2 + " " + result3 + " " + result4
  }

  /**
   * Hashes an input string into a base 16 md5 string
   */
  def md5(text: String): String = {
    return MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
  }

  /**
   * Takes the text of the tweet and checks if it has already been put
   * in the database. Checks both hash of tweet and link inside tweet.
   */
  def checkTweetExists(text: String): Boolean = {
    var linkMatch: Option[DBObject] = None
    var hashMatch: Option[DBObject] = coll.findOne(DBObject("hash" -> md5(text)))
    try {
      linkMatch = coll.findOne(DBObject("link"-> getLinks(text).head))
    } catch {
      case e: Exception => linkMatch = None
    }
    linkMatch match {
      case Some(_) => return true
      case _ => hashMatch match {
        case Some(_) => return true
        case _ => return false
      }
    }
  }

  /**
   * Tries to check if the tweet is posted by an aggregation website
   * returns true if it is, or false if there is no supporting evidence
   */
  def checkIfAggregator(status: Status): Boolean = {
    val userMap = userToMap(status.getUser.toString)
    val source = status.getSource.toString.toLowerCase
    // if the reference list of job sites contain a link in the source, then true
    for (site <- jobsites) {
      if (source.contains(site)) return true
    }
    return false
  }

  /**
   * Converts a serialized object into an Object
   * User will have to cast the object into specific object
   */
  def SToO(str: String): Object = {
    var out: Object = null
    if (str != null) {
      try {
        val bios: ByteArrayInputStream = new ByteArrayInputStream(decode
        .decodeBuffer(str))
        val ois: ObjectInputStream = new ObjectInputStream(bios)
        out = ois.readObject()
        } catch {
        case e: Exception =>
        e.printStackTrace
        return null
        }
    }
     return out
  }

}
