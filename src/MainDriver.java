import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.Controller;
import bolt.DBolt;
import bolt.UBolt;
import io.Parameters;
import spout.RedisQueueSpout;
import util.RedisCleanUp;
import util.ReportManager;
import util.WriteDataToRedis;

import java.io.IOException;

/**
 * Created by Roy Gao on 1/13/2016.
 */
public class MainDriver {

	// strom jar MainDriver.jar MainDriver
	// [task-name] load-balance  [local|remote] remote   ...
	// default: local mode

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		ReportManager manager = new ReportManager();

		Config conf = new Config();
		conf.setDebug(true);

		if (args.length == 0) {
			// default: local mode
			setup(builder, manager, Parameters.LOCAL_HOST);

			manager.setLimit(Parameters.LOCAL_TIME);
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(Parameters.DEFAULT_TOPOLOGY_NAME, conf, builder.createTopology());

			Thread thread = new Thread(manager);
			thread.setDaemon(true);
			thread.start();

			Thread.sleep(Parameters.LOCAL_TIME);
			cluster.shutdown();

		} else if (args.length > 1) {
			String taskName = args[0];
			if (args[1].equals("local")) // local mode
				startTopology(builder, manager, Parameters.LOCAL_HOST, conf, taskName);
			else if (args[1].equals("remote")) // remote mode
				startTopology(builder, manager, Parameters.REMOTE_HOST, conf, taskName);
			else errorArgs();

		} else errorArgs();
	}

	private static void setup(TopologyBuilder builder, ReportManager manager, String mode) throws IOException {
		WriteDataToRedis.writeToRedis(Parameters.LOCAL_HOST, Parameters.REDIS_PORT);
		RedisCleanUp.redisCleanUp(Parameters.LOCAL_HOST);

		builder.setSpout(Parameters.SPOUT_NAME, new RedisQueueSpout(mode, Parameters.REDIS_PORT), 1);

		builder.setBolt(Parameters.UBOLT_NAME,
				new UBolt(mode, Parameters.REDIS_PORT), 10).shuffleGrouping(Parameters.SPOUT_NAME);
		builder.setBolt(Parameters.DBOLT_NAME,
				new DBolt(mode, Parameters.REDIS_PORT), 10).directGrouping(Parameters.UBOLT_NAME);
		builder.setBolt(Parameters.CONTROLLER_NAME,
				new Controller(mode, Parameters.REDIS_PORT), 1).directGrouping(Parameters.DBOLT_NAME);

		manager.initialize(mode, Parameters.REDIS_PORT, 10);
	}

	private static void startTopology(TopologyBuilder builder, ReportManager manager, String mode,
	                                  Config conf, String taskName) throws Exception {
		setup(builder, manager, mode);

		conf.setNumWorkers(10);
		StormSubmitter.submitTopologyWithProgressBar(taskName, conf, builder.createTopology());

		Thread thread = new Thread(manager);
		// thread.setDaemon(true);
		thread.start();
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
		System.out.println("Usage: storm jar MainDriver.jar MainDriver [task-name] [local|remote] ...");
	}
}
