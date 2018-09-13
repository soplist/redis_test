package com.redis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.SortingParams;

/** 
 * @author 作者 E-mail: 
 * @version 创建时间：2018-9-13 上午9:03:57 
 * 类说明 
 */
public class RedisClient {
    private Jedis jedis;
    private JedisPool jedisPool;
    private ShardedJedis shardedJedis;
    private ShardedJedisPool shardedJedisPool;
    
    public RedisClient(){
    	initialPool();
    	initialShardedPool();
    	shardedJedis = shardedJedisPool.getResource();
    	jedis = jedisPool.getResource();
    }
    
    private void initialPool(){
    	JedisPoolConfig config = new JedisPoolConfig();
    	config.setMaxActive(20);
    	config.setMaxIdle(5);
    	config.setMaxWait(10001);
    	config.setTestOnBorrow(false);
    	
    	jedisPool = new JedisPool(config,"127.0.0.1",6379,10000,"123456");
    	
    }
    
    private void initialShardedPool(){
    	JedisPoolConfig config = new JedisPoolConfig();
    	config.setMaxActive(20);
    	config.setMaxIdle(5);
    	config.setMaxWait(10001);
    	config.setTestOnBorrow(false);
    	
    	List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
    	JedisShardInfo jedisShardInfo = new JedisShardInfo("127.0.0.1",6379,"master");
    	jedisShardInfo.setPassword("123456");
    	shards.add(jedisShardInfo);
    	
    	
    	shardedJedisPool = new ShardedJedisPool(config,shards);
    }
    
    public void show(){
    	//KeyOperate();
    	//StringOperate();
    	//ListOperate();
    	//SetOperate();
    	SortedSetOperate();
    	HashOperate();
    	jedisPool.returnResource(jedis);
    	shardedJedisPool.returnResource(shardedJedis);
    }
    
    private void KeyOperate(){
    	System.out.println("===============key===============");
    	
    	System.out.println("flush DB:"+jedis.flushDB());
    	System.out.println("judge key 'key999' is exists or not:"+shardedJedis.exists("key999"));
    	System.out.println("judge key 'name' is exists or not:"+shardedJedis.exists("name"));
    	System.out.println("add key001,value001 key-value:"+shardedJedis.set("key001", "value001"));
    	System.out.println("judge key 'key001' is exists or not:"+shardedJedis.exists("key001"));
    	System.out.println("add key002,value002 key-value:"+shardedJedis.set("key002", "value002"));
    	System.out.println("add key003,value003 key-value:"+jedis.set("key003", "value003"));
    	System.out.println("judge key 'key003' is exists or not:"+shardedJedis.exists("key003"));
    	System.out.print("all keys:");
    	Set<String> keys = jedis.keys("*");
    	Iterator<String> it=keys.iterator();
    	while(it.hasNext()){
    		String key = it.next();
    		System.out.print(key+".");
    	}
    	System.out.println("");
    	System.out.println("delete key002 from system:"+jedis.del("key002"));
    	System.out.println("judge key 'key002' is exists or not:"+shardedJedis.exists("key002"));
    	System.out.println("set key001's expire time 5's:"+jedis.expire("key001", 5));
    	try{ 
            Thread.sleep(2000); 
        } 
        catch (InterruptedException e){ 
        } 
    	System.out.println("key001's surplus time:"+jedis.ttl("key001"));
    	System.out.println("judge key 'key001' is exists or not:"+shardedJedis.exists("key001"));
    	System.out.println("'key001' type:"+jedis.type("key001"));
    	try{ 
            Thread.sleep(4000); 
        } 
        catch (InterruptedException e){ 
        } 
    	System.out.println("judge key 'key001' is exists or not:"+shardedJedis.exists("key001"));
    	
    }
    
    private void StringOperate(){
        System.out.println("===============string===============");
    	System.out.println("flush DB:"+jedis.flushDB());
    	System.out.println("add");
    	jedis.set("key001", "value001");
    	jedis.set("key002", "value002");
    	jedis.set("key003", "value003");
    	System.out.print("already add:");
    	System.out.print(jedis.get("key001")+".");
    	System.out.print(jedis.get("key002")+".");
    	System.out.print(jedis.get("key003")+".");
    	System.out.println("");
    	System.out.println("delete");
    	System.out.println("delete key003:"+jedis.del("key003"));
    	System.out.println("get key003 value:"+jedis.get("key003")+".");
    	System.out.println("update");
    	System.out.println("update key001:"+jedis.set("key001", "value001-update"));
    	System.out.println("get key001 value:"+jedis.get("key001")+".");
    	System.out.println("append key002:"+jedis.append("key002", "-append"));
    	System.out.println("get key002 value:"+jedis.get("key002")+".");
    	
    	System.out.println("flush DB:"+jedis.flushDB());
    	System.out.println("key301 not exist,add key301:"+shardedJedis.setnx("key301", "value301"));
    	System.out.println("key302 not exist,add key302:"+shardedJedis.setnx("key302", "value302"));
    	System.out.println("key302 exist,add key302:"+shardedJedis.setnx("key302", "value302_new"));
    	System.out.println("get key301 value:"+jedis.get("key301")+".");
    	System.out.println("get key302 value:"+jedis.get("key302")+".");
    	
    	System.out.println("add key303,and set expire time 2's:"+shardedJedis.setex("key303", 2, "key303-2second"));
    	System.out.println("get key303:"+shardedJedis.get("key303"));
    	try{ 
            Thread.sleep(3000); 
        } 
    	catch (InterruptedException e){ 
        } 
    	System.out.println("get key303:"+shardedJedis.get("key303"));
    	
    	System.out.println("key302 old value:"+shardedJedis.getSet("key302","value302-after-getset"));
    	System.out.println("key302 new value:"+shardedJedis.get("key302"));
    	
    	System.out.println("get key302's range:"+shardedJedis.getrange("key302",5,7));
    	
    	System.out.println("add in one time:"+jedis.mset("key201","value201","key202","value202","key203","value203","key204","value204"));
    	System.out.println("get in one time:"+jedis.mget("key201","key202","key203","key204"));
    	System.out.println("delete in one time:"+jedis.del(new String[]{"key201","key202","key203","key204"}));
    	System.out.println("get in one time:"+jedis.mget("key201","key202","key203","key204"));
    }
    
    private void ListOperate(){
    	System.out.println("flush DB:"+jedis.flushDB());
    	shardedJedis.lpush("stringlists", "vector"); 
        shardedJedis.lpush("stringlists", "ArrayList"); 
        shardedJedis.lpush("stringlists", "vector");
        shardedJedis.lpush("stringlists", "vector");
        shardedJedis.lpush("stringlists", "LinkedList");
        shardedJedis.lpush("stringlists", "MapList");
        shardedJedis.lpush("stringlists", "SerialList");
        shardedJedis.lpush("stringlists", "HashList");
        shardedJedis.lpush("numberlists", "3");
        shardedJedis.lpush("numberlists", "1");
        shardedJedis.lpush("numberlists", "5");
        shardedJedis.lpush("numberlists", "2");
        System.out.println("all elements-stringlists："+shardedJedis.lrange("stringlists", 0, -1));
        System.out.println("all elements-numberlists："+shardedJedis.lrange("numberlists", 0, -1));
        
        System.out.println("delete-stringlists："+shardedJedis.lrem("stringlists", 2, "vector")); 
        System.out.println("all elements-stringlists："+shardedJedis.lrange("stringlists", 0, -1));
        System.out.println("delete element out of range 0-3:"+shardedJedis.ltrim("stringlists", 0, 3));
        System.out.println("all elements-stringlists:"+shardedJedis.lrange("stringlists", 0, -1));
        
        System.out.println("pop element:"+shardedJedis.lpop("stringlists")); 
        System.out.println("all elements-stringlists:"+shardedJedis.lrange("stringlists", 0, -1));
        
        shardedJedis.lset("stringlists", 0, "hello list!"); 
        System.out.println("all elements-stringlists:"+shardedJedis.lrange("stringlists", 0, -1));
        
        System.out.println("lengh-stringlists："+shardedJedis.llen("stringlists"));
        System.out.println("lengh-numberlists："+shardedJedis.llen("numberlists"));
        
        SortingParams sortingParameters = new SortingParams();
        sortingParameters.alpha();
        sortingParameters.limit(0, 3);
        System.out.println("after sort-stringlists："+shardedJedis.sort("stringlists",sortingParameters)); 
        System.out.println("after sort-numberlists："+shardedJedis.sort("numberlists"));
        
        System.out.println("substring from second element:"+shardedJedis.lrange("stringlists", 1, -1));
        
        System.out.println("get element index 2:"+shardedJedis.lindex("stringlists", 2)+"\n");
    }
    
    private void SetOperate(){
    	System.out.println(jedis.flushDB()); 
    	System.out.println("add element001 to sets:"+jedis.sadd("sets", "element001")); 
        System.out.println("add element002 to sets:"+jedis.sadd("sets", "element002")); 
        System.out.println("add element003 to sets:"+jedis.sadd("sets", "element003"));
        System.out.println("add element004 to sets:"+jedis.sadd("sets", "element004"));
        System.out.println("get all elements:"+jedis.smembers("sets")); 
        
        System.out.println("delete element003："+jedis.srem("sets", "element003"));
        System.out.println("get all elements:"+jedis.smembers("sets")); 
        
        System.out.println("add element001 to sets1:"+jedis.sadd("sets1", "element001")); 
        System.out.println("add element002 to sets1:"+jedis.sadd("sets1", "element002")); 
        System.out.println("add element003 to sets1:"+jedis.sadd("sets1", "element003")); 
        System.out.println("add element002 to sets2:"+jedis.sadd("sets2", "element002")); 
        System.out.println("add element003 to sets2:"+jedis.sadd("sets2", "element003")); 
        System.out.println("add element004 to sets2:"+jedis.sadd("sets2", "element004"));
        System.out.println("elements in sets1:"+jedis.smembers("sets1"));
        System.out.println("elements in sets2:"+jedis.smembers("sets2"));
        
        System.out.println("sets1 and sets2 sinter:"+jedis.sinter("sets1", "sets2"));
        System.out.println("sets1 and sets2 sunion:"+jedis.sunion("sets1", "sets2"));
        System.out.println("sets1 and sets2 sdiff:"+jedis.sdiff("sets2", "sets1"));
    }
    
    private void SortedSetOperate(){
    	System.out.println(jedis.flushDB());
    	
    	System.out.println("zset add element001:"+shardedJedis.zadd("zset", 7.0, "element001")); 
        System.out.println("zset add element002:"+shardedJedis.zadd("zset", 8.0, "element002")); 
        System.out.println("zset add element003:"+shardedJedis.zadd("zset", 2.0, "element003")); 
        System.out.println("zset addelement004:"+shardedJedis.zadd("zset", 3.0, "element004"));
        System.out.println("all elements in zset:"+shardedJedis.zrange("zset", 0, -1));
        
        System.out.println("delete element002:"+shardedJedis.zrem("zset", "element002"));
        System.out.println("all elements in zset:"+shardedJedis.zrange("zset", 0, -1));
        
        System.out.println("size of zset:"+shardedJedis.zcard("zset"));
        System.out.println("size of zset weight.1.0-5.0:"+shardedJedis.zcount("zset", 1.0, 5.0));
        System.out.println("element004 weight:"+shardedJedis.zscore("zset", "element004"));
        System.out.println("value from index 1 to 2:"+shardedJedis.zrange("zset", 1, 2));
    }
    
    private void HashOperate(){
    	System.out.println(jedis.flushDB()); 
    	System.out.println("hashs add key001-value001:"+shardedJedis.hset("hashs", "key001", "value001")); 
        System.out.println("hashs add key002-value002:"+shardedJedis.hset("hashs", "key002", "value002")); 
        System.out.println("hashs add key003-value003:"+shardedJedis.hset("hashs", "key003", "value003"));
        System.out.println("add key004-4:"+shardedJedis.hincrBy("hashs", "key004", 4l));
        System.out.println("all elements in hashs:"+shardedJedis.hvals("hashs"));
        System.out.println("delete key002:"+shardedJedis.hdel("hashs", "key002"));
        System.out.println("all elements in hashs:"+shardedJedis.hvals("hashs"));
        System.out.println("key004 add 100:"+shardedJedis.hincrBy("hashs", "key004", 100l));
        System.out.println("all elements in hashs:"+shardedJedis.hvals("hashs"));
        System.out.println("judge key003 exist or not:"+shardedJedis.hexists("hashs", "key003"));
        System.out.println("get key004 value:"+shardedJedis.hget("hashs", "key004"));
        System.out.println("get key001 and key003value:"+shardedJedis.hmget("hashs", "key001", "key003")); 
        System.out.println("get all keys:"+shardedJedis.hkeys("hashs"));
        System.out.println("get all values:"+shardedJedis.hvals("hashs"));
    }
}
