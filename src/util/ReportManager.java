package util;

import io.Parameters;
import redis.clients.jedis.Jedis;

/**
 * Created by Roy Gao on 1/14/2016.
 */
public class ReportManager implements Runnable {

	private String host;
	int port;
	int DBoltNumber;
	int limit = -1;

	public ReportManager() {
		host = Parameters.REDIS_REMOTE;
		port = Parameters.REDIS_PORT;
	}

	public void initialize(String host, int port, int DBoltNumber) {
		this.host = host;
		this.port = port;
		this.DBoltNumber = DBoltNumber;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	@Override
	public void run() {
		Jedis jedis = new Jedis(host, port);
		long start = System.currentTimeMillis(), last = start;
		while (true) {
			for (int i = 0; i < 1000; ++i) ;
			if (System.currentTimeMillis() - last > 10000) {
				for (int i = 0; i < DBoltNumber; ++i)
					jedis.lpush(Parameters.REDIS_LOAD + i, "");

				if (limit > 0 && System.currentTimeMillis() - start > limit)
					break;

				last = System.currentTimeMillis();
			}
		}
	}
}
