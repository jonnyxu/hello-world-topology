package com.jonny.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.jonny.storm.bolt.HelloWorldBolt;
import com.jonny.storm.spout.HelloWorldSpout;

/**
 * <p>Title: HelloWorldTopology</p>
 * <p>Description: A Storm Topology for Hello World.</p>
 * <p>Copyright: Copyright (c) 2013</p>
 * <p>Company: Covisint LLC</p>
 * @author Jonny Xu
 * @date Jul 20, 2017
 * @version 1.0
 */

public class HelloWorldTopology {

	/**
	 *  Running Topology on local cluster:
	 *  	 mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.jonny.storm.HelloWorldTopology
	 *  
	 *  Deploy Topology to Storm cluster:
	 *  	./storm/bin/storm jar hello-world-topology-1.0-SNAPSHOT.jar com.jonny.storm.HelloWorldTopology HelloWorldTopology
	 *  
	 *  Stopping Storm Topology:
	 *  	./storm/bin/storm kill HelloWorldTopology
	 *  
	 * @param args
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws AuthorizationException
	 */

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		/**
		 * 定义拓扑
		 */
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("HelloWorldSpout", new HelloWorldSpout(), 3);

		/**
		 *  在spout和bolts之间通过shuffleGrouping方法连接。这种分组方式决定了Storm会以随机分配方式从源节点向目标节点发送消息。
		 *  
		 *  Storm中8种流分组的方式：
		 *  fieldsGrouping（字段分组）：根据指定字段对流进行分组
		 *  globalGrouping（全局分组）：全部流发送到同一个Bolt中
		 *  shuffleGrouping（随机分组）：最常用的分组方式，随机分发元组
		 *  localOrShuffleGrouping（本地或者随机分组）：如果目标Bolt在同一工作进程存在一个或多个任务，会随机分配元组给这些任务
		 *  noneGrouping（无分组）：同随机分组
		 *  allGrouping（广播分组）：将分发流到所有的Bolt中，常用于更新缓存
		 *  directGrouping（直接分组）：只能在已声明为直接流的流中所使用，并且元组必须使用emitDirect方法来发射
		 *  customGrouping（自定义分组）：实现CustomStreamGrouping接口来创建自定义的流分组
		 */
		builder.setBolt("HelloWorldBolt", new HelloWorldBolt(), 2).shuffleGrouping("HelloWorldSpout");

		/**
		 * 配置拓扑
		 * 由于是在开发阶段，设置debug属性为true，Storm会打印节点间交换的所有消息，以及其它有助于理解拓扑运行方式的调试数据
		 */
		Config conf = new Config();
		conf.setDebug(true);

		/**
		 * 运行拓扑
		 */
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

}
