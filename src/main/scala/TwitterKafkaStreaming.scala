import java.time.Duration

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters._
import KafkaStreaming._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils

import SparkBigData._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Minutes

// développer des clients streaming qui consomment les données de Twitter et les poussent dans Kafka
class TwitterKafkaStreaming {

  private var trace_client_streaming : Logger = LogManager.getLogger("Log_Console")

  /**
   *  Client Spark Streaming Twitter Kafka. Ce client Spark Streaming se connecte à Twitter et publie les infos dans Kafka via un Producer Kafka
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCESS_TOKEN
   * @param TOKEN_SECRET
   * @param filtre
   * @param kafkaBootStrapServers
   * @param topic
   */
  def ProducerTwitterKafkaSpark (CONSUMER_KEY : String, CONSUMER_SECRET : String, ACCESS_TOKEN : String, TOKEN_SECRET : String, filtre : Array[String],
                                 kafkaBootStrapServers : String, topic : String) : Unit = {

    val authO = new OAuthAuthorization(twitterOAuthConf(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET).build())

    val client_Streaming_Twitter = TwitterUtils.createStream(getSparkStreamingContext(true, 15), Some(authO), filtre)

    val tweetsmsg = client_Streaming_Twitter.flatMap(status => status.getText())
    val tweetsComplets = client_Streaming_Twitter.flatMap(status => (status.getText() ++ status.getContributors() ++ status.getLang()))
    val tweetsFR = client_Streaming_Twitter.filter(status => status.getLang() == "fr")
    val hastags = client_Streaming_Twitter.flatMap(status => status.getText().split(" ").filter(status => status.startsWith("#")))
    val hastagsFR = tweetsFR.flatMap(status => status.getText().split(" ").filter(status => status.startsWith("#")))
    val hastagsCount = hastagsFR.window(Minutes(3))

    // ATTENTION à cette erreur !!! getProducerKafka(kafkaBootStrapServers, topic, tweetsmsg.toString())

    // tweetsmsg.saveAsTextFiles("Tweets")

    tweetsmsg.foreachRDD {
      (tweetsRDD, temps) =>
        if (!tweetsRDD.isEmpty()) {
          tweetsRDD.foreachPartition {
            partitionsOfTweets =>
              val producer_Kafka = new KafkaProducer[String, String](getKafkaProducerParams(kafkaBootStrapServers))
              partitionsOfTweets.foreach {
                tweetEvent =>
                  val record_publish = new ProducerRecord[String, String](topic, tweetEvent.toString)
                  producer_Kafka.send(record_publish)

              }
              producer_Kafka.close()
          }
        }
    }

    try {
      tweetsComplets.foreachRDD {
        tweetsRDD =>
          if (!tweetsRDD.isEmpty()) {
            tweetsRDD.foreachPartition{
              tweetsPartition => tweetsPartition.foreach{ tweets =>
                getProducerKafka(kafkaBootStrapServers, topic, tweets.toString )
              }
            }
          }
          getProducerKafka(kafkaBootStrapServers, "", "").close()
      }
    }
    catch {
      case ex : Exception => trace_client_streaming.error(ex.printStackTrace())
    }

    getSparkStreamingContext(true, 15).start()
    getSparkStreamingContext(true, 15).awaitTermination()

    // pour arrêter le contexte Spark Streaming : getSparkStreamingContext(true, 15).stop()

  }


  // exemple de spécification d'un paramètre optionel en scala
  test(Some(true), 15)

  // exemple de spécification d'un paramètre optionel en scala
  def test (var1 : Option[Boolean], param2 : Int): Unit = {
    println(var1)

  }


}
