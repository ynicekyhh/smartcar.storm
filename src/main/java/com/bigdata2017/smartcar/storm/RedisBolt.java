package com.bigdata2017.smartcar.storm;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisBolt extends AbstractRedisBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisBolt.class);		

	public RedisBolt( JedisClusterConfig  config ) {
		super( config );
	}

	public RedisBolt( JedisPoolConfig config ) {
		super( config );
	}
	
	@Override
	public void execute( Tuple tuple ) {

		JedisCommands jedisCommands = null;
		
		try {
				
			String date = tuple.getStringByField( "date" );
			String carNumber = tuple.getStringByField( "car_number" );
			
			LOGGER.error( "RedisBolt:execute[" + date + ":" + carNumber );
	
			jedisCommands = getInstance();
			jedisCommands.sadd( date, carNumber );
			jedisCommands.expire( date, 60 * 60 * 24 * 7 );
	
		} catch (JedisConnectionException e) {
			throw new RuntimeException( "Exception occurred to JedisConnection", e );
		} catch (JedisException e) {
			System.out.println( "Exception occurred from Jedis/Redis" + e );
		} finally {
			if ( jedisCommands != null ) {
				returnInstance( jedisCommands );
			}
			
			collector.ack( tuple );
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
}
