package com.jonny.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * <p>Title: HelloWorldBolt</p>
 * <p>Description: Storm Bolt component for Hello World Topology.</p>
 * <p>Copyright: Copyright (c) 2013</p>
 * <p>Company: Covisint LLC</p>
 * @author Jonny Xu
 * @date Jul 20, 2017
 * @version 1.0
 */

public class HelloWorldBolt extends BaseRichBolt {

	private static final long	serialVersionUID	= 1L;

	private int					myCount;

	private OutputCollector		collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	/**
	 * 从Spout中接收文本，并进行计数。
	 */
	@Override
	public void execute(Tuple input) {

		String sentence = input.getStringByField("sentence");
		System.out.println("Input from Spout is: [" + sentence + "]");

		if ("Hello World".equals(sentence)) {
			myCount++;
			System.out.println("Found a Hello World! My Count is now: " + Integer.toString(myCount));
		}

		/**
		 *  对元组进行应答 
		 *  Storm提供了另外一个用来实现bolt的接口-IBasicBolt 和 抽象类-BaseBasicBolt。对于该接口的实现类的对象，它会在执行execute方法之后自动调用ack方法。
		 */
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
