package com.velankani.apps.connectxml
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF

import java.io.FileOutputStream
import java.io.ObjectOutputStream

/**
 * Takes training data provided by the python script to fetch tweets in that specific format.
 * Does Naive Bayes training on it and saves the model to the specified output directory.
 * Should be able to get over 95% accuracy.
 */
object NaiveBayesTrain {

    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println("Usage: " + this.getClass.getSimpleName +
                " <tweetInput> <outputModelDir>")
            System.exit(1)
        }
        val Array(tweetInput, outputModelDir) = args

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)

        val documents = sc.textFile(tweetInput)
        // ratings are the 1/0 values to say whether it's a job or not
        val ratings = documents.map(_.split("\\|#\\|")(5))
        // transforms the documents to a sequence of strings
        val docsToSeq = documents.map(_.split("\\|#\\|")(4)
            .replaceAll("""http:\/\/[a-zA-Z\/.0-9]*""", "")
            .replaceAll("""[\p{Punct}&&[^#]]""", "")
            .split("\\s").map(x => PorterStemmer.stem(x)).toSeq)
        val hashingTF = new HashingTF()
        // term frequency vectors for each doc
        val tf = hashingTF.transform(docsToSeq)

        tf.cache()

        val idf = new IDF().fit(tf)
        val tfidf = idf.transform(tf)
        val ratingsToVectors = ratings.zip(tfidf)

        val parsedData = ratingsToVectors.map(x => LabeledPoint(x._1.toDouble, x._2))
        val splits = parsedData.randomSplit(Array(0.7, 0.3))
        val training = splits(0)
        val test = splits(1)

        val model = NaiveBayes.train(training, lambda = 0.0001, modelType = "multinomial")

        val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
        val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
        println(accuracy)
        model.save(sc, outputModelDir)
        // Try get get higher than 0.95
    }
    
}
