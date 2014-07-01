package org.apache.accumulo.storagehandler;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class AccumuloHiveRecordWriter implements RecordWriter {
  
  private static final Logger log = Logger.getLogger(AccumuloHiveRecordWriter.class);
  static {
    log.setLevel(Level.INFO);
  }
  
  @Override
  public void close(boolean arg0) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(Writable writable) throws IOException {
    // TODO Auto-generated method stub
    log.info("in recordwriter " + writable.getClass());
    MapWritable mapWritable = (MapWritable)writable;
    
  }

}
