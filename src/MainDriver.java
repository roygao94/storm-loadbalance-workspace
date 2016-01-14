import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.Controller;
import bolt.DBolt;
import bolt.UBolt;
import io.Parameters;
import spout.RedisQueueSpout;
import util.ReportManager;
import util.WriteDataToRedis;

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
			WriteDataToRedis.writeToRedis(Parameters.REDIS_LOCAL, Parameters.REDIS_PORT);

			setTopology(builder, manager, Parameters.REDIS_LOCAL);
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("load-balance-driver", conf, builder.createTopology());

			manager.run();

			Thread.sleep(30000);
			cluster.shutdown();

		} else if (args.length > 1) {
			String taskName = args[0];
			if (args[1].equals("local")) {
				// local mode
				WriteDataToRedis.writeToRedis(Parameters.REDIS_LOCAL, Parameters.REDIS_PORT);

				setTopology(builder, manager, Parameters.REDIS_LOCAL);
				conf.setNumWorkers(10);
				StormSubmitter.submitTopologyWithProgressBar(taskName, conf, builder.createTopology());

				manager.run();

			} else if (args[1].equals("remote")) {
				// remote mode
				WriteDataToRedis.writeToRedis(Parameters.REDIS_REMOTE, Parameters.REDIS_PORT);

				setTopology(builder, manager, Parameters.REDIS_REMOTE);
				conf.setNumWorkers(10);
				StormSubmitter.submitTopologyWithProgressBar(taskName, conf, builder.createTopology());

				manager.run();

			} else errorArgs();

		} else errorArgs();
	}

	private static void setTopology(TopologyBuilder builder, ReportManager manager, String mode) {
		builder.setSpout("spout", new RedisQueueSpout(mode, Parameters.REDIS_PORT), 1);

		builder.setBolt("u-bolt", new UBolt(mode, Parameters.REDIS_PORT), 10).shuffleGrouping("spout");
		builder.setBolt("d-bolt", new DBolt(mode, Parameters.REDIS_PORT), 10).directGrouping("u-bolt");
		builder.setBolt("controller", new Controller(mode, Parameters.REDIS_PORT), 1).directGrouping("d-bolt");

		manager.initialize(mode, Parameters.REDIS_PORT, 10);
	}

	private static void errorArgs() {
		System.out.println("Usage: storm jar MainDriver.jar MainDriver {task-name} {local|remote} ...");
	}
}
