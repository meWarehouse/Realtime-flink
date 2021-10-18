package com.at.retime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author zero
 * @create 2021-05-21 16:09
 */
public class MyRedisUtil {

    private static JedisPool jedisPool;


    public static Jedis getJedis(){
        if(jedisPool == null){
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(100); //最大可用连接数
            config.setBlockWhenExhausted(true); //连接耗尽是否等待
            config.setMaxWaitMillis(2000); //等待时间
            config.setMaxIdle(5); //最大闲置连接数
            config.setMinIdle(5); //最小闲置连接数
            config.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            jedisPool = new JedisPool(config,"hadoop102",6379,10000);
        }
        return jedisPool.getResource();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
    }


}
