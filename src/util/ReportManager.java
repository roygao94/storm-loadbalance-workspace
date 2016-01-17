package util;

import conf.Parameters;
import redis.clients.jedis.Jedis;

/**
 * Created by Roy Gao on 1/14/2016.
 */
public class ReportManager implements Runnable {

	private String host;
	private int port = Parameters.REDIS_PORT;
	int DBoltNumber;
	long limit = -1;

//	public ReportManager() {
//		host = Parameters.REMOTE_HOST;
//		port = Parameters.REDIS_PORT;
//	}

	public void initialize(Parameters parameters, int DBoltNumber) {
		this.host = parameters.HOST;
		this.DBoltNumber = DBoltNumber;
	}

	public void setLimit(long limit) {
		this.limit = limit;
	}

	@Override
	public void run() {
		Jedis jedis = new Jedis(host, port);
		long start = System.currentTimeMillis(), last = start;
		while (true) {
			for (int i = 0; i < 1000; ++i) ;
			if (System.currentTimeMillis() - last > Parameters.REPORT_TIME) {
				for (int i = 0; i < DBoltNumber; ++i)
					jedis.lpush(Parameters.REDIS_LOAD + i, "");

				if (limit > 0 && System.currentTimeMillis() - start > limit)
					break;

				last = System.currentTimeMillis();
			}
		}
	}
}
