package com.bigdata2017.smartcar.storm;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class HBaseBolt implements IRichBolt {

	private static final String TABLE_NAME = "table_smartcar_driving";
	private static final String ZOOKEEPER_QUORUM = "server02.hadoop.com";
	private static final String ZOOKEEPER_CLIENT_PORT = "2181";

	private static final String TUPLE_ROW_KEY_FIELD = "r_key";		
	private static final String COLUMN_FAMILY = "cf";
//	private static final String TUPLE_TIMESTAMP_FIELD = "";
	private static final boolean IS_BATCH = false;
	
	private static final String[] COLUMN_NAMES = { "date", "car_number", "speed_pedal", "break_pedal", "steer_angle", "direct_light", "speed", "area_number" };
	
	private OutputCollector collector;
	private HTableConnector connector;
	private boolean autoAck = true;

	@Override
	@SuppressWarnings( "rawtypes" )	
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		try {
			this.connector = new HTableConnector();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void execute(Tuple tuple) {
		try {
			connector.getTable().put( getPutFromTuple( tuple ) );
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		if( autoAck ) {
			collector.ack( tuple );
		}		
	}


	@Override
	public void cleanup() {
		connector.close();		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	private Put getPutFromTuple(final Tuple tuple) {
		long ts = 0;
		byte[] rowKey = Bytes.toBytes( tuple.getStringByField( TUPLE_ROW_KEY_FIELD ) );
//		if (!TUPLE_TIMESTAMP_FIELD.equals( "" )) {
//			ts = tuple.getLongByField( TUPLE_TIMESTAMP_FIELD );
//		}

		Put p = new Put( rowKey );
		p.setWriteToWAL(true);

		byte[] cfBytes = Bytes.toBytes( COLUMN_FAMILY );
		for( String cq : COLUMN_NAMES ) {
			byte[] cqBytes = Bytes.toBytes( cq );
			byte[] val = Bytes.toBytes( tuple.getStringByField( cq ) );
			if (ts > 0) {
				p.add( cfBytes, cqBytes, ts, val );
			} else {
				p.add( cfBytes, cqBytes, val );
			}
		}

		return p;
	}

	
	private class HTableConnector implements Serializable {

		private HTable hTable;
		
		public HTableConnector() throws IOException {
			try {
				Configuration config = HBaseConfiguration.create();
				config.set( "hbase.zookeeper.quorum", ZOOKEEPER_QUORUM );
				config.set( "hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENT_PORT );
				config.set( "hbase.cluster.distributed", "true" );
	
				hTable = new HTable( config, TABLE_NAME );
	
				if ( IS_BATCH ) {
					hTable.setAutoFlush( false, true );
				}
	
				// Check the configured column families exist
				if ( !columnFamilyExists( COLUMN_FAMILY ) ) {
					throw new RuntimeException( String.format("HBase table '%s' does not have column family '%s'", TABLE_NAME, COLUMN_FAMILY ));
				}
			} catch (IOException ex) {
				throw new IOException( "Unable to establish connection to HBase table "	+ TABLE_NAME, ex );
			}
		}

		private boolean columnFamilyExists( final String columnFamily ) throws IOException {
			return hTable.getTableDescriptor().hasFamily( Bytes.toBytes( columnFamily ) );
		}

		public HTable getTable() {
			return hTable;
		}

		public void close() {
			try {
				if( hTable != null ) {
					hTable.close();
				}
			} catch( IOException ex ) {
				ex.printStackTrace();
			}
		}
	}
}