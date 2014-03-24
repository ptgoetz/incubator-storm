# Storm Kafka Spout Example

This is a simple example intended for smoke testing the Storm Kafka spout.

## Usage

Install and run kafka per the instructions found here [http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)

Make note of the hostname of the server where zookeeper is running.

### Create the Kafka Topic

```
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic
```

### Running the Topology in local mode

To run in local mode, use the following command:

```
java -cp storm-kafka-example-0.9.1-incubating-jar-with-dependencies.jar org.apache.storm.kafka.TestKafkaTopology <zkHosts>
```

Where `zkHosts` is the connect string for your zookeeper quorum.

### Running the Topology in Distributed Mode

To run in distributed mode, use the following command:

```
java -cp storm-kafka-example-0.9.1-incubating-jar-with-dependencies.jar org.apache.storm.kafka.TestKafkaTopology <zkHosts> <topologyName>
```
Where `zkHosts` is the connect string for your zookeeper quorum, and `topologyName` is the name you would like to give the topology.



### Send Messages to the Kafka Queue

Send messages to the queue by running the kafka console producer:

```
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
```

Enter some text, followed by <RETURN>.

Verify that the text you entered appears in the storm log, e.g.:

```
DEBUG: [hello world]
```
