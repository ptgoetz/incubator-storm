package org.apache.storm.kafka;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.*;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

public class TestKafkaTopology {

    public static StormTopology buildTopology(String zkHosts) {
        TridentTopology topology = new TridentTopology();

        BrokerHosts zk = new ZkHosts(zkHosts);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "test-topic");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        Stream spoutStream = topology.newStream("kafka-stream", spout);

        spoutStream.each(spoutStream.getOutputFields(), new Debug());


        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        conf.setMaxSpoutPending(5);
        if (args.length == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-test", conf, buildTopology(args[0]));

        } else if (args.length == 2) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[1], conf, buildTopology(args[0]));
        } else {
            System.out.println("Usage: TestKafkaTopology <zookeeper hosts> [topology name]");
            System.out.println("If [topology name] is specified, the topology will run on a remote cluster.");
        }
    }
}
