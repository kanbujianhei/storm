package com.yijiupi.himalaya.Bolt;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yijiupi.himalaya.pojo.RowMap;

public class DatabseFilterBolt extends BaseRichBolt {
	private String DATABASE_01 = "yjp_order_1";
	private String DATABASE_02 = "yjp_order_2";
	private String DATABASE_03 = "*";
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String msg = input.getString(0);
		ObjectMapper mapper = new ObjectMapper();
		try {
			RowMap rowMap = mapper.readValue(msg, RowMap.class);
			if (DATABASE_01 == rowMap.getDatabase() || DATABASE_02 == rowMap.getDatabase()
					|| DATABASE_03 == rowMap.getDatabase()) {
				collector.emit(new Values(rowMap));
			}
		} catch (IOException e) {
			// TODO
		}
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("database"));
		System.out.println("DataseFilterBolt");
	}

}
