package org.brett.noworlater;

import redis.clients.jedis.*;

/**
 * Created by bwarminski on 10/29/17.
 */
public class JedisHelper {
    public static Long saddString(Jedis jedis, String key, String... member) {
        return jedis.sadd(key, member);
    }

    public static Response<Long> saddStringPipeline(Pipeline jedis, String key, String... member) {
        return jedis.sadd(key, member);
    }
}
