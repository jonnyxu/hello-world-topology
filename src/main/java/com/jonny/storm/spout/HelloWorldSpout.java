package com.jonny.storm.spout;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * <p>Title: HelloWorldSpout</p>
 * <p>Description: Storm Spout component for Hello World Topology.</p>
 * <p>Copyright: Copyright (c) 2013</p>
 * <p>Company: Covisint LLC</p>
 * @author Jonny Xu
 * @date Jul 20, 2017
 * @version 1.0
 */

public class HelloWorldSpout extends BaseRichSpout {

	private static final long		serialVersionUID	= 1L;

	private SpoutOutputCollector	collector;
	private int						referenceRandom;
	private static final int		MAX_RANDOM			= 10;

	public HelloWorldSpout() {
		final Random rand = new Random();
		referenceRandom = rand.nextInt(MAX_RANDOM);
	}

	/**
	 * 维持一个collector对象 
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	/**
	 * 这个方法做的惟一一件事情就是发布文本
	 * 我们要通过它向bolts发布待处理的数据
	 * NOTE: Values是一个ArrarList实现，它的元素就是传入构造器的参数。
	 * 而且nextTuple()会在同一个循环内被ack()和fail()周期性的调用，没有任务时它必须释放对线程的控制，其它方法才有机会得以执行。
	 */
	@Override
	public void nextTuple() {

		Utils.sleep(60000);
		final Random rand = new Random();
		int instanceRandom = rand.nextInt(MAX_RANDOM);

		/**
		 * 发布一个新值
		 */
		if (instanceRandom == referenceRandom) {
			collector.emit(new Values("Hello World"));
		} else {
			collector.emit(new Values("Other Random Word"));
		}

	}

	/**
	 * 声明输出参数: "sentence"
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
