package org.apache.accumulo.storagehandler;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class AccumuloHiveRecordWriter implements RecordWriter {
  
  private static final Logger log = Logger.getLogger(AccumuloHiveRecordWriter.class);
  static {
    log.setLevel(Level.INFO);
  }
  private JobConf jobConf;
  private Properties properties;
  private Instance instance;
  
  public AccumuloHiveRecordWriter(JobConf jobConf, Properties properties) {
    this.jobConf = jobConf;
    this.properties = properties;
  }

  private Instance getInstance(String id, String zookeepers) {
    if (instance != null) {
      return instance;
    } else {
      return new ZooKeeperInstance(id, zookeepers);
    }
  }
  
  @Override
  public void close(boolean arg0) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(Writable writable) throws IOException {
    log.info("writing AccumuloHiveRow to accumulo " + writable.getClass());
    
    if(!(writable instanceof AccumuloHiveRow)){
      try {
        throw new SerDeException(getClass().getName() + " : " +
            "Expected AccumuloHiveRow. Got " + writable.getClass().getName());
      } catch (SerDeException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    log.info("jobConf " + jobConf.toString());
    log.info("properties " + properties.toString());
    // get accumulo connection
    Connector connector = AccumuloHiveUtils.getConnector(jobConf);
    log.info("instance =  " + connector.getInstance().getInstanceName());
    // implement a batchwriter next! 
    AccumuloHiveRow row = (AccumuloHiveRow)writable;
    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    try {
      BatchWriter batchWriter = connector.createBatchWriter(properties.getProperty(AccumuloSerde.TABLE_NAME), batchWriterConfig);
      // create mutation with rowid
      Mutation mutation = new Mutation(new Text(row.getRowId()));
      for(AccumuloHiveRow.ColumnTuple tuple : row.tuples){
        // add accumulo record
        mutation.put(tuple.getCf(), tuple.getQual(), new Value(tuple.getValue()));
      }
      batchWriter.addMutation(mutation);
      batchWriter.close();
    } catch (TableNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (MutationsRejectedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }

}
