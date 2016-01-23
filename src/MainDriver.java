import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.Controller;
import bolt.DBolt;
import bolt.UBolt;
import conf.Parameters;
import spout.RedisQueueSpout;
import tools.RedisCleaner;
import tools.ReportManager;
import tools.RedisWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Roy Gao on 1/13/2016.
 */
public class MainDriver {

	/*
	strom jar MainDriver.jar MainDriver
	[task-name] load-balance  [local|remote] remote  [ignore|balance] ignore...
	default: local mode
	*/

	public static void main(String[] args) throws Exception {

		Parameters parameters = new Parameters();
		parameters.setHost(Parameters.LOCAL_HOST);

		TopologyBuilder builder = new TopologyBuilder();
		ReportManager manager = new ReportManager();

		Config conf = new Config();
		conf.setDebug(true);

		if (args.length == 0) {
			// default: local mode
			File tempDir = new File(parameters.getBaseDir() + parameters.getTopologyName());
			if (!tempDir.exists())
				tempDir.mkdirs();

			setup(builder, parameters);

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
			parameters.setTopologyName(args[0]);

			if (args[1].equals("local")) {// local mode
				parameters.setLocalMode();
				parameters.appendRedisHead("L-");
			} else if (args[1].equals("remote")) {// remote mode
				parameters.setRemoteMode();
				parameters.appendRedisHead("R-");
			} else errorArgs();

			if (args[2].equals("ignore")) {
				parameters.setBalance(false);
				parameters.appendRedisHead("I-");
			} else if (args[2].equals("balance")) {
				parameters.setBalance(true);
				parameters.appendRedisHead("B-");
			}

			if (args[2].equals("balance")) {
				if (args.length > 3)
					try {
						double balanceIndex = Double.parseDouble(args[3]);
						if (balanceIndex > 0 || balanceIndex < 1)
							parameters.setBalanceIndex(balanceIndex);
						else {
							errorArgs();
							return;
						}
					} catch (Exception e) {
						errorArgs();
						return;
					}
			}


			File tempDir = new File(parameters.getBaseDir() + parameters.getTopologyName());
			if (!tempDir.exists())
				tempDir.mkdirs();
			BufferedWriter writer = new BufferedWriter(
					new FileWriter(parameters.getBaseDir() + parameters.getTopologyName() + "/rebalance.txt"));
			writer.write("--ms\n");
			writer.write("--");
			writer.close();

			BufferedWriter writer2 = new BufferedWriter(
					new FileWriter(parameters.getBaseDir() + parameters.getTopologyName() + "/migration.txt"));
			writer2.close();

			startTopology(builder, manager, conf, parameters);

		} else errorArgs();
	}

	private static void setup(TopologyBuilder builder, Parameters parameters) throws IOException {
		RedisWriter.writeToRedis(parameters.getHost(), Parameters.REDIS_PORT);
		RedisCleaner.redisCleanUp(parameters);

		builder.setSpout(Parameters.SPOUT_NAME, new RedisQueueSpout(parameters), 1);

		builder.setBolt(Parameters.UBOLT_NAME,
				new UBolt(parameters), 10).shuffleGrouping(Parameters.SPOUT_NAME);
		builder.setBolt(Parameters.DBOLT_NAME,
				new DBolt(parameters), 10).directGrouping(Parameters.UBOLT_NAME);

//		if (parameters.BALANCE)
		builder.setBolt(Parameters.CONTROLLER_NAME,
				new Controller(parameters), 1).directGrouping(Parameters.DBOLT_NAME);
	}

	private static void startTopology(TopologyBuilder builder, ReportManager manager,
	                                  Config conf, Parameters parameters) throws Exception {
		setup(builder, parameters);

		conf.setNumWorkers(10);
		StormSubmitter.submitTopologyWithProgressBar(parameters.getTopologyName(), conf, builder.createTopology());

//		if (parameters.BALANCE) {
		manager.initialize(parameters, 10);

		Thread thread = new Thread(manager);
		// thread.setDaemon(true);
		thread.start();
//		}
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
		System.out.println("Usage: storm jar MainDriver.jar "
				+ "MainDriver [task-name] [local|remote] [ignore|balance] [balance-index] ...");
	}
}
