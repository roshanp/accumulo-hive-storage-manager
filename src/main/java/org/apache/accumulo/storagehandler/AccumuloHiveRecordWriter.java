package org.apache.accumulo.storagehandler;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.MapWritable;
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
      
    // configure an accumulo job to insert 
    log.info("jobConf " + jobConf.toString());
    log.info("properties " + properties.toString());
    String user = jobConf.get(AccumuloSerde.USER_NAME);
    String pass = jobConf.get(AccumuloSerde.USER_PASS);
    String id = jobConf.get(AccumuloSerde.INSTANCE_ID);
    String zookeepers = jobConf.get(AccumuloSerde.ZOOKEEPERS);
    instance = getInstance(id, zookeepers);
    log.info("user " + user + " pass " + pass + "  id " + zookeepers + " instance " + instance);
    // implement a batchwriter next!  
    
  }

}
