import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.DBolt;
import bolt.UBolt;
import spout.RedisQueueSpout;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class SimpleTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RedisQueueSpout("10.11.1.56", 6379, "100000_0.85"), 5);

		builder.setBolt("ubolt", new UBolt(), 10).shuffleGrouping("spout");
		builder.setBolt("dbolt", new DBolt(), 10).directGrouping("ubolt");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(10);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("simple-tpop", conf, builder.createTopology());

			Thread.sleep(30000);

			cluster.shutdown();
		}
	}
}
