package stream.common.util.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

/**
 * JedisSentinelPool连接池的方式调用redis数据库 
 * 
 */
public class JedisPoolUtil {
    
    private final static Logger logger = Logger.getLogger(JedisPoolUtil.class.getName());
    
    private static JedisSentinelPool pool = null;
 
    public static Properties getJedisProperties() {
        Properties config = new Properties();
        InputStream is = null;
        try {
            is = JedisPoolUtil.class.getClassLoader().getResourceAsStream("config.properties");
            config.load(is);
        } catch (IOException e) {
            logger.info(e.getMessage());
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.info(e.getMessage());
                }
            }
        }
        return config;
    }
 
    /**
     * 创建连接池
     *
     */
    private static void createJedisPool() {
        // 建立连接池配置参数
        JedisPoolConfig config = new JedisPoolConfig();
        Properties prop = getJedisProperties();
        // 设置最大连接数
        config.setMaxTotal(Integer.valueOf(prop.getProperty("redis.MAX_ACTIVE")));
        // 设置最大阻塞时间，记住是毫秒数milliseconds
        config.setMaxWaitMillis(Integer.valueOf(prop.getProperty("redis.MAX_WAIT")));
        // 设置空间连接
        config.setMaxIdle(Integer.valueOf(prop.getProperty("redis.MAX_IDLE")));
        // jedis实例是否可用
        boolean borrow = prop.getProperty("redis.TEST_ON_BORROW") == "false" ? false : true;
        config.setTestOnBorrow(borrow);
        
        String masterName = "master001";
        Set<String> sentinels = new HashSet<String>();
        
        String serviceAddressPort1 = prop.getProperty("redis.ADRESS") + ":" + prop.getProperty("redis.PORT1");
        String serviceAddressPort2 = prop.getProperty("redis.ADRESS") + ":" + prop.getProperty("redis.PORT2");
        String serviceAddressPort3 = prop.getProperty("redis.ADRESS") + ":" + prop.getProperty("redis.PORT3");
        sentinels.add(serviceAddressPort1);
        sentinels.add(serviceAddressPort2);
        sentinels.add(serviceAddressPort3);
        pool = new JedisSentinelPool(masterName, sentinels, config);
    }
 
    /**
     * 在多线程环境同步初始化
     */
    private static synchronized void poolInit() {
        if (pool == null)
            createJedisPool();
    }
 
    /**
     * 获取一个jedis 对象
     *
     * @return
     */
    public static Jedis getJedis() {
        if (pool == null)
            poolInit();
        return pool.getResource();
    }
 
    /**
     * 释放一个连接
     *
     * @param jedis
     */
    @SuppressWarnings("deprecation")
    public static void returnRes(Jedis jedis) {
        pool.returnResource(jedis);
    }
 
    /**
     * 销毁一个连接
     *
     * @param jedis
     */
    @SuppressWarnings("deprecation")
    public static void returnBrokenRes(Jedis jedis) {
        pool.returnBrokenResource(jedis);
    }
     
     
    public static void main(String[] args){
        Jedis jedis = getJedis();
        //默认是0数据库
        Set<String> keySet = jedis.keys("DP*");
        for(String str: keySet){
            System.out.println(str);
            String value = jedis.get(str);
            System.out.println(value);
        }
        //选择2数据库
        jedis.select(2);
        jedis.set("DP1502246220191", "hahahaha");
        String value = jedis.get("DP1502246220191");
        System.out.println(value);
        jedis.del("DP1502246220191");
        //释放资源
        JedisPoolUtil.returnRes(jedis);
    }
}
