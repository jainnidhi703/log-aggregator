# Log Aggregator

`log-aggregator` aggregates logs based on `HttpMethod` on a minute window.


## High Level Design
![alt text](https://s9.postimg.org/wvg4evgkv/cloudcraft_-_Log_Aggregator_2.png "Log Aggregator")


- `MicroServices` can publish all the logs to particular Kafka Topic. 
- `Flink Streaming` application can subscribe to the topic and aggregate per minute on HttpMethod type. Storm can also be used as an aggregator with metrics: TimeStamp(minutes) and HttpMethod(Get/Put/Post/Delete) and dimensions as Total Count.
- The processed logs can be stored in a DB or ElasticSearch/Kibana for better visualization.


## Low-Level Design

### Queue

- A publisher/subscriber messaging can act as a middleware to manage logs from all services ensuring reliability and fault tolerance.

#### Why Kafka?

- There are many choices for pub-sub messaging systems, so what makes Apache Kafka a good choice?
- `Multiple Producers`: Kafka is able to seamlessly handle multiple producers which make the system ideal for aggregating data from many MicroServices
- `Multiple Consumers`: Kafka is designed for multiple consumers to read a single stream of messages without interfering with each other.
- `Scalable and Fault Tolerance`: In Kafka, Fault tolerance is provided by replication, and scalability is provided by partitions of data across a topic. 


### Processing

Logs have to be aggregated per minute based on method type in real time. One of the following approaches can be used.

#### KafkaConsumer

The client consumes and processes records from a Kafka cluster.
```java
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(60000); //1 minute window
    for (ConsumerRecord<String, String> record : records){
        String[] logValues = record.value().split("-");
        String methodType = logValues[3];
    }
}
```
#### Flink Streaming 

Flink Streaming can also be used to aggregate the logs based on HttpType and then Sink it to the DB every minute.

```java
StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<String>(kafkaTopic,
		new SimpleStringSchema(), kafkaConsumerProperties);
DataStream<String> dataStream = environment.addSource(kafkaSource);
//Processing
DataStream<String> processedData = dataStream.map(new MapFunction<String, String>() {});
processedData.addSink(new SinkFunction<String>() {
	@Override
	public void invoke(String s) throws Exception {
	//aggregate for a minute and then sink
	}
});
```

#### Storm 
A Storm topology can be also be used by configuring Count metric and HttpMethod & Time as dimensions. Storm provides the primitives for transforming and aggregating a stream into a new stream in a distributed and reliable way.

### Output

- SQL can be used as the output is already structured in the format as follow: 

| Time | Get       | Put       | Post      | Delete    |
| ---|---|---|---|---|
| 2017-06-10T18:40| 200 | 456 | 345 | 8|
| 2017-06-10T18:41| 120 | 356 | 122 | 80|

- Another approach can be indexing the data to Elastic Search/Kibana for better visualization.
