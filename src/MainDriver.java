import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.Controller;
import bolt.DBolt;
import bolt.UBolt;
import conf.Parameters;
import spout.RedisQueueSpout;
import util.RedisCleaner;
import util.ReportManager;
import util.RedisWriter;

import java.io.IOException;

/**
 * Created by Roy Gao on 1/13/2016.
 */
public class MainDriver {

	// strom jar MainDriver.jar MainDriver
	// [task-name] load-balance  [local|remote] remote  [ignore|balance] ignore...
	// default: local mode

	public static void main(String[] args) throws Exception {

		Parameters parameters = new Parameters();

		TopologyBuilder builder = new TopologyBuilder();
		ReportManager manager = new ReportManager();

		Config conf = new Config();
		conf.setDebug(true);

		if (args.length == 0) {
			// default: local mode
			setup(builder, manager, parameters);

			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(Parameters.DEFAULT_TOPOLOGY_NAME, conf, builder.createTopology());
//
//			manager.setLimit(Parameters.LOCAL_TIME);
//			Thread thread = new Thread(manager);
//			thread.setDaemon(true);
//			thread.start();

			Thread.sleep(Parameters.LOCAL_TIME);
			cluster.shutdown();

		} else if (args.length > 2) {
			parameters.TOPOLOGY_NAME = args[0];

			if (args[1].equals("local")) // local mode
				parameters.HOST = Parameters.LOCAL_HOST;
			else if (args[1].equals("remote")) // remote mode
				parameters.HOST = Parameters.REMOTE_HOST;
			else errorArgs();

			if (args[2].equals("ignore"))
				parameters.BALANCE = false;
			else if (args[2].equals("balance"))
				parameters.BALANCE = true;


			startTopology(builder, manager, conf, parameters);

		} else errorArgs();
	}

	private static void setup(TopologyBuilder builder, ReportManager manager, Parameters parameters) throws IOException {
		RedisWriter.writeToRedis(parameters.HOST, Parameters.REDIS_PORT);
		RedisCleaner.redisCleanUp(parameters.HOST);

		builder.setSpout(Parameters.SPOUT_NAME, new RedisQueueSpout(parameters), 1);

		builder.setBolt(Parameters.UBOLT_NAME,
				new UBolt(parameters), 10).shuffleGrouping(Parameters.SPOUT_NAME);
		builder.setBolt(Parameters.DBOLT_NAME,
				new DBolt(parameters), 10).directGrouping(Parameters.UBOLT_NAME);

		if (parameters.BALANCE)
			builder.setBolt(Parameters.CONTROLLER_NAME,
					new Controller(parameters), 1).directGrouping(Parameters.DBOLT_NAME);

//			manager.initialize(parameters, 10);
	}

	private static void startTopology(TopologyBuilder builder, ReportManager manager,
	                                  Config conf, Parameters parameters) throws Exception {
		setup(builder, manager, parameters);

		conf.setNumWorkers(10);
		StormSubmitter.submitTopologyWithProgressBar(parameters.TOPOLOGY_NAME, conf, builder.createTopology());

		if (parameters.BALANCE) {
			Thread thread = new Thread(manager);
			// thread.setDaemon(true);
			thread.start();
		}
	}

//	private static void setTopology(TopologyBuilder builder, ReportManager manager, String mode) {
//		builder.setSpout(Parameters.SPOUT_NAME, new RedisQueueSpout(mode, Parameters.REDIS_PORT), 1);
//
//		builder.setBolt(Parameters.UBOLT_NAME,
//				new UBolt(mode, Parameters.REDIS_PORT), 10).shuffleGrouping(Parameters.SPOUT_NAME);
//		builder.setBolt(Parameters.DBOLT_NAME,
//				new DBolt(mode, Parameters.REDIS_PORT), 10).directGrouping(Parameters.UBOLT_NAME);
//		builder.setBolt(Parameters.CONTROLLER_NAME,
//				new Controller(mode, Parameters.REDIS_PORT), 1).directGrouping(Parameters.DBOLT_NAME);
//
//		manager.initialize(mode, Parameters.REDIS_PORT, 10);
//	}

	private static void errorArgs() {
		System.out.println("Usage: storm jar MainDriver.jar MainDriver [task-name] [local|remote] [ignore|balance]...");
	}
}
