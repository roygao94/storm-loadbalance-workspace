import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.DBolt;
import bolt.UBolt;
import io.Parameters;
import spout.RedisQueueSpout;
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
		Config conf = new Config();
		conf.setDebug(true);

		if (args.length == 0) {
			// default: local mode
			WriteDataToRedis.writeToRedis(Parameters.REDIS_LOCAL, Parameters.REDIS_PORT);

			setTopology(builder, Parameters.REDIS_LOCAL);
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("load-balance-driver", conf, builder.createTopology());

			Thread.sleep(30000);
			cluster.shutdown();

		} else if (args.length > 1) {
			String taskName = args[0];
			if (args[1].equals("local")) {
				// local mode
				WriteDataToRedis.writeToRedis(Parameters.REDIS_LOCAL, Parameters.REDIS_PORT);

				setTopology(builder, Parameters.REDIS_LOCAL);
				conf.setNumWorkers(10);
				StormSubmitter.submitTopologyWithProgressBar(taskName, conf, builder.createTopology());

			} else if (args[1].equals("remote")) {
				// remote mode
				WriteDataToRedis.writeToRedis(Parameters.REDIS_REMOTE, Parameters.REDIS_PORT);

				setTopology(builder, Parameters.REDIS_REMOTE);
				conf.setNumWorkers(10);
				StormSubmitter.submitTopologyWithProgressBar(taskName, conf, builder.createTopology());

			} else errorArgs();

		} else errorArgs();
	}

	private static void setTopology(TopologyBuilder builder, String mode) {
		builder.setSpout("spout", new RedisQueueSpout(mode, Parameters.REDIS_PORT), 1);

		builder.setBolt("u-bolt", new UBolt(mode, Parameters.REDIS_PORT), 10).shuffleGrouping("spout");
		builder.setBolt("d-bolt", new DBolt(mode, Parameters.REDIS_PORT), 10).directGrouping("u-bolt");
	}

	private static void errorArgs() {
		System.out.println("Usage: storm jar MainDriver.jar MainDriver {task-name} {local|remote} ...");
	}
}
