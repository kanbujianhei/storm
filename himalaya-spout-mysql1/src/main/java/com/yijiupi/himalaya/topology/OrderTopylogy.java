package com.yijiupi.himalaya.topology;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.yijiupi.himalaya.Bolt.DatabseFilterBolt;
import com.yijiupi.himalaya.Bolt.InsertFilterBolt;
import com.yijiupi.himalaya.Bolt.TableFilterBolt;

public class OrderTopylogy {
	public static void main(String[] args) {
		String zkhost = "172.16.1.57:2181,172.16.1.58:2181,172.16.1.59:2181";
		String topic = "maxwell";
		String groupId = "trading-group";
		int spoutNum = 1;
		int boltNum = 1;
		ZkHosts zkHosts = new ZkHosts(zkhost);// kafaka所在的zookeeper
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/kafka", groupId);
		spoutConfig.zkPort = 2181;
		spoutConfig.zkServers = Arrays.asList("172.16.1.57", "172.16.1.58", "172.16.1.59", "172.16.1.78");

		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", kafkaSpout, 3);
		builder.setBolt("database-filter", new DatabseFilterBolt(), 3).shuffleGrouping("kafka-reader");
		builder.setBolt("table-filter", new TableFilterBolt(), 3).shuffleGrouping("database-filter");
		builder.setBolt("insert-filter", new InsertFilterBolt(), 3).shuffleGrouping("table-filter");
		Config conf = new Config();
		conf.setDebug(true);
		// 设置任务线程数
		conf.setMaxTaskParallelism(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
	}
}
