import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.DBolt;
import bolt.UBolt;
import io.Parameters;
import spout.RedisQueueSpout;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class SimpleTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RedisQueueSpout(Parameters.REDIS_REMOTE, Parameters.REDIS_PORT), 1);

		builder.setBolt("u-bolt",
				new UBolt(Parameters.REDIS_REMOTE, Parameters.REDIS_PORT), 10).shuffleGrouping("spout");
		builder.setBolt("d-bolt",
				new DBolt(Parameters.REDIS_REMOTE, Parameters.REDIS_PORT), 10).directGrouping("u-bolt");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(10);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("simple-topo", conf, builder.createTopology());

			Thread.sleep(30000);

			cluster.shutdown();
		}
	}
}
