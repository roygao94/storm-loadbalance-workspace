package util;

import io.Parameters;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Roy Gao on 1/13/2016.
 */
public class WriteDataToRedis {

	private String host;
	private int port;

	public static void main(String[] args) throws IOException {
		writeToRedis(Parameters.LOCAL_HOST, Parameters.REDIS_PORT);
//		writer.writeToRedis();
	}

	public WriteDataToRedis(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public static void writeToRedis(String host, int port) throws IOException {
		String line;
		int val;
		Jedis jedis = new Jedis(host, port);

		if (!jedis.exists(Parameters.REDIS_KGS)) {
			BufferedReader reader = new BufferedReader(new FileReader("equal-10000.txt"));
			List<Integer> gList = new ArrayList<>();

			while ((line = reader.readLine()) != null) {
				val = Integer.parseInt(line);
				gList.add(val);
			}
			reader.close();

			for (int i = 0; i < gList.size(); ++i)
				jedis.lpush(Parameters.REDIS_KGS, i + "," + gList.get(i));
		}
		jedis.disconnect();
	}

}
