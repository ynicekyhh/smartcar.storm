package com.bigdata2017.smartcar.storm;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class HBaseBolt implements IRichBolt {

	protected OutputCollector collector;
	protected HTableConnector connector;
	protected TupleTableConfig conf;
	protected boolean autoAck = true;

	public HBaseBolt(TupleTableConfig conf) {
		this.conf = conf;
	}

	@SuppressWarnings("rawtypes")	
	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		try {
			this.connector = new HTableConnector(conf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void execute(Tuple tuple) {
		try {
			this.connector.getTable().put(conf.getPutFromTuple(tuple));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		if (this.autoAck) {
			this.collector.ack(tuple);
		}		
	}


	@Override
	public void cleanup() {
		this.connector.close();		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	private class HTableConnector implements Serializable {

		private Configuration config;
		protected HTable table;
		private String tableName;

		public HTableConnector(final TupleTableConfig conf) throws IOException {
			
			this.tableName = conf.getTableName();
			this.config = HBaseConfiguration.create();
			this.config.set("hbase.zookeeper.quorum", conf.getZkQuorum());
			this.config.set("hbase.zookeeper.property.clientPort", conf.getZkClientPort());
			this.config.set("hbase.cluster.distributed", "true");

			try {
				this.table = new HTable(this.config, this.tableName);
			} catch (IOException ex) {
				throw new IOException( "Unable to establish connection to HBase table "	+ this.tableName, ex);
			}

			if (conf.isBatch()) {
				this.table.setAutoFlush(false, true);
			}

			// If set, override write buffer size
			if (conf.getWriteBufferSize() > 0) {
				try {
					this.table.setWriteBufferSize(conf.getWriteBufferSize());
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}

			// Check the configured column families exist
			for (String cf : conf.getColumnFamilies()) {
				if (!columnFamilyExists(cf)) {
					throw new RuntimeException(String.format("HBase table '%s' does not have column family '%s'", conf.getTableName(), cf));
				}
			}
		}

		private boolean columnFamilyExists(final String columnFamily)
				throws IOException {
			return this.table.getTableDescriptor().hasFamily(Bytes.toBytes(columnFamily));
		}

		public HTable getTable() {
			return table;
		}

		public void close() {
			try {
				this.table.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	private class TupleTableConfig implements Serializable {

		private String tableName;
		private String zkQuorum;
		private String zkClientPort;

		protected String tupleRowKeyField;
		protected String tupleTimestampField;
		protected Map<String, Set<String>> columnFamilies;
		private boolean batch = true;
		protected boolean writeToWAL = true;
		private long writeBufferSize = 0L;

		public TupleTableConfig(final String table, final String rowKeyField) {
			this.tableName = table;
			this.tupleRowKeyField = rowKeyField;
			this.tupleTimestampField = "";
			this.columnFamilies = new HashMap<String, Set<String>>();
		}

		public TupleTableConfig(final String table, final String rowKeyField, final String timestampField) {
			this.tableName = table;
			this.tupleRowKeyField = rowKeyField;
			this.tupleTimestampField = timestampField;
			this.columnFamilies = new HashMap<String, Set<String>>();
		}

		public void addColumn(final String columnFamily, final String columnQualifier) {
			Set<String> columns = this.columnFamilies.get(columnFamily);

			if (columns == null) {
				columns = new HashSet<String>();
			}
			columns.add(columnQualifier);

			this.columnFamilies.put(columnFamily, columns);
		}

		public Put getPutFromTuple(final Tuple tuple) {

			byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));

			long ts = 0;
			if (!tupleTimestampField.equals("")) {
				ts = tuple.getLongByField(tupleTimestampField);
			}

			Put p = new Put(rowKey);
			p.setWriteToWAL(writeToWAL);

			if (columnFamilies.size() > 0) {
				for (String cf : columnFamilies.keySet()) {
					byte[] cfBytes = Bytes.toBytes(cf);
					for (String cq : columnFamilies.get(cf)) {
						byte[] cqBytes = Bytes.toBytes(cq);
						byte[] val = Bytes.toBytes(tuple.getStringByField(cq));

						if (ts > 0) {
							p.add(cfBytes, cqBytes, ts, val);
						} else {
							p.add(cfBytes, cqBytes, val);
						}
					}
				}
			}

			return p;
		}

		public Increment getIncrementFromTuple(final Tuple tuple, final long increment) {
			byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));

			Increment inc = new Increment(rowKey);
			inc.setWriteToWAL(writeToWAL);

			if (columnFamilies.size() > 0) {
				for (String cf : columnFamilies.keySet()) {
					byte[] cfBytes = Bytes.toBytes(cf);
					for (String cq : columnFamilies.get(cf)) {
						byte[] val;
						try {
							val = Bytes.toBytes(tuple.getStringByField(cq));
						} catch (IllegalArgumentException ex) {
							// if cq isn't a tuple field, use cq for counter instead
							// of tuple
							// value
							val = Bytes.toBytes(cq);
						}
						inc.addColumn(cfBytes, val, increment);
					}
				}
			}

			return inc;
		}

		public String getTableName() {
			return tableName;
		}

		public boolean isBatch() {
			return batch;
		}

		public void setBatch(boolean batch) {
			this.batch = batch;
		}

		public void setWriteToWAL(boolean writeToWAL) {
			this.writeToWAL = writeToWAL;
		}

		public boolean isWriteToWAL() {
			return writeToWAL;
		}

		public void setWriteBufferSize(long writeBufferSize) {
			this.writeBufferSize = writeBufferSize;
		}

		public long getWriteBufferSize() {
			return writeBufferSize;
		}

		public Set<String> getColumnFamilies() {
			return this.columnFamilies.keySet();
		}

		public String getTupleRowKeyField() {
			return tupleRowKeyField;
		}

		public String getZkClientPort() {
			return zkClientPort;
		}

		public String getZkQuorum() {
			return zkQuorum;
		}

		public void setZkClientPort(String zkClientPort) {
			this.zkClientPort = zkClientPort;
		}

		public void setZkQuorum(String zkQuorum) {
			this.zkQuorum = zkQuorum;
		}
	}	

}
