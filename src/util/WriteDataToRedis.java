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
		WriteDataToRedis writer = new WriteDataToRedis();
		writer.writeToRedis();
	}

	private WriteDataToRedis() {
		host = Parameters.localMode ? Parameters.REDIS_VM : Parameters.REDIS_REMOTE;
		port = Parameters.REDIS_PORT;
	}

	public void writeToRedis() throws IOException {
		String line;
		int val;
//		System.out.println(host);
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
