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
  
  public AccumuloHiveRecordWriter(JobConf jobConf, Properties properties) {
    this.jobConf = jobConf;
    this.properties = properties;
  }

  @Override
  public void write(Writable writable) throws IOException {
    // get accumulo connection
    log.info("here");

    Connector connector = AccumuloHiveUtils.getConnector(jobConf);
    log.info("here");
    // write accumulo records
    AccumuloHiveRow row = (AccumuloHiveRow)writable;
    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    try {
      BatchWriter batchWriter = connector.createBatchWriter(properties.getProperty(AccumuloSerde.TABLE_NAME), batchWriterConfig);
      // create mutation with rowid
      Mutation mutation = new Mutation(new Text(row.getRowId()));
      for(AccumuloHiveRow.ColumnTuple tuple : row.tuples){
        // add each tuple
        mutation.put(tuple.getCf(), tuple.getQual(), new Value(tuple.getValue()));
      }
      batchWriter.addMutation(mutation);
      batchWriter.close();
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }
    
  }

  @Override
  public void close(boolean arg0) throws IOException {
    // TODO Auto-generated method stub
    
  }

}
