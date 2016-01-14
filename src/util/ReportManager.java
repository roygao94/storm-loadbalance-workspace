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

	public ReportManager() {
		host = Parameters.REDIS_REMOTE;
		port = Parameters.REDIS_PORT;
	}

	public void initialize(String host, int port, int DBoltNumber) {
		this.host = host;
		this.port = port;
		this.DBoltNumber = DBoltNumber;
	}

	@Override
	public void run() {
		Jedis jedis = new Jedis(host, port);

		while (true) {
			try {
				Thread.sleep(10000);
				for (int i = 0; i < DBoltNumber; ++i)
					jedis.lpush(Parameters.REDIS_LOAD + i);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
