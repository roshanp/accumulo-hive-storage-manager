package org.apache.accumulo.storagehandler;

import java.io.IOException;
import java.util.Properties;

import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.integration.junit4.JMockit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(JMockit.class)
public class AccumuloHiveRecordWriterTest {

  @Test
  public void testWrite(@Mocked final AccumuloHiveRecordWriter recordWriter,
      @Mocked final JobConf jobConf,
      @Mocked final Properties properties,
      @Mocked AccumuloHiveUtils accumuloHiveUtils,
      @Mocked final Connector connector, 
      @Mocked final AccumuloHiveRow row,
      @Mocked final BatchWriterConfig batchWriterConfig, 
      @Mocked final BatchWriter batchWriter,
      @Mocked final Mutation mutation,
      @Mocked final AccumuloHiveRow.ColumnTuple tuple) throws Exception {
       
    new NonStrictExpectations() {{
      connector.createBatchWriter(properties.getProperty(AccumuloSerde.TABLE_NAME), withInstanceOf(BatchWriterConfig.class));
      result=batchWriter;
      batchWriter.addMutation(mutation);
      batchWriter.close();
    }};
    
    recordWriter.write(row);
    
  }

}
