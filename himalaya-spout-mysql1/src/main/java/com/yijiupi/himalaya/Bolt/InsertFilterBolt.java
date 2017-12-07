package com.yijiupi.himalaya.Bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.yijiupi.himalaya.pojo.RowMap;

public class InsertFilterBolt extends BaseRichBolt {
	private OutputCollector collector;
	private String TYPE = "*";

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		RowMap rowMap = (RowMap) input.getValueByField("table");
		if (TYPE == rowMap.getType()) {
			System.out.println("----------------" + rowMap.toString() + "------------------");
			collector.emit(new Values(rowMap));
		}
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("insertFilterBolt");
	}
}
