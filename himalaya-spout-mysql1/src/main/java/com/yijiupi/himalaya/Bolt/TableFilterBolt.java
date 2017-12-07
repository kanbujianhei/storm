package com.yijiupi.himalaya.Bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.yijiupi.himalaya.pojo.RowMap;

public class TableFilterBolt extends BaseRichBolt {
	private OutputCollector collector;
	private String TABLE = "*";

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		RowMap rowMap = (RowMap) input.getValueByField("database");
		if (TABLE == rowMap.getTable()) {
			collector.emit(new Values(rowMap));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("table"));
		System.out.println("tableFilterBolt");
	}
}
