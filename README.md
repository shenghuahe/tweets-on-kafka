# tweets-on-kafka
Stream tweets from Twitter into Kafka and consume them back

### Why Kafka and tweets?
Kafka is one of the many distributed message brokers that enables low latency message
ingestion. Ingesting data such as tweets in real-time can be very useful. In this case,
I could do some analysis in near real-time downstream using Spark or Beam to understand 
what people are talking about regarding weather and maybe retarget users with offers etc.


### Useful reading
1. Kafka in Docker: https://github.com/wurstmeister/kafka-docker 
1. Python Kafka Producer & Consumer: https://kafka-python.readthedocs.io/en/master/usage.html
1. Streaming tweets using Tweepy: https://tweepy.readthedocs.io/en/3.7.0/streaming_how_to.html
