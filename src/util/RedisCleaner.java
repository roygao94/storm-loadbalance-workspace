package util;

import conf.Parameters;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Created by roy on 1/15/16.
 */
public class RedisCleaner {

	public static void main(String[] args) {
		redisCleanUp(Parameters.REMOTE_HOST);
	}

	public static void redisCleanUp(String host) {
		Jedis jedis = new Jedis(host, Parameters.REDIS_PORT);

//		for (int i = 0; i < 10; ++i) {
//			if (jedis.exists(Parameters.REDIS_LOAD + i))
//				jedis.del(Parameters.REDIS_LOAD + i);
//			if (jedis.exists(Parameters.REDIS_DETAIL + i))
//				jedis.del(Parameters.REDIS_DETAIL + i);
//			if (jedis.exists("emit-" + Parameters.REDIS_LOAD + i))
//				jedis.del("emit-" + Parameters.REDIS_LOAD + i);
//			if (jedis.exists("balanced"))
//				jedis.del("balanced");
//			if (jedis.exists("imbalanced"))
//				jedis.del("imbalanced");
//		}

		Set<String> keys = jedis.keys("*");
		for (String key : keys)
			if (key.startsWith(Parameters.REDIS_LOAD) || key.startsWith(Parameters.REDIS_DETAIL)
					|| key.startsWith(Parameters.REDIS_LOAD_REPORT) || key.startsWith(Parameters.REDIS_DETAIL_REPORT)
					|| key.startsWith(Parameters.REDIS_RT) || key.startsWith("debug")
					|| key.startsWith("balanced") || key.startsWith("imbalanced") || key.startsWith("rebalanced"))
				jedis.del(key);

		jedis.disconnect();
	}
}
