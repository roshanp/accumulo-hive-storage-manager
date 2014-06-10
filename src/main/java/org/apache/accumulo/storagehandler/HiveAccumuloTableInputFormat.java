package org.apache.accumulo.storagehandler;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.storagehandler.predicate.AccumuloPredicateHandler;
import org.apache.accumulo.storagehandler.predicate.PrimitiveComparisonFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

/**
 * Wraps older InputFormat for use with Hive.
 *
 * Configure input scan with proper ranges, iterators, and columns based on
 * serde properties for Hive table.
 */
public class HiveAccumuloTableInputFormat
        extends AccumuloRowInputFormat
        implements org.apache.hadoop.mapred.InputFormat<Text, AccumuloHiveRow> {

  private static final Pattern PIPE = Pattern.compile("[|]");
  public static final Text EMPTY_CQ = new Text();
  private AccumuloPredicateHandler predicateHandler = AccumuloPredicateHandler.getInstance();
    private Instance instance;

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        String id = jobConf.get(AccumuloSerde.INSTANCE_ID);
        String user = jobConf.get(AccumuloSerde.USER_NAME);
        String pass = jobConf.get(AccumuloSerde.USER_PASS);
        String zookeepers = jobConf.get(AccumuloSerde.ZOOKEEPERS);
        instance = getInstance(id, zookeepers);
        Job job = new Job(jobConf);
        try {
            Connector connector =  instance.getConnector(user,  new PasswordToken(pass.getBytes()));
            String colMapping = jobConf.get(AccumuloSerde.COLUMN_MAPPINGS);
            List<String> colQualFamPairs = AccumuloHiveUtils.parseColumnMapping(colMapping);
            configure(job, jobConf, connector, colQualFamPairs);
            List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);
            int incForRowID = AccumuloHiveUtils.containsRowID(colMapping) ? 1 : 0;
            if (colQualFamPairs.size() + incForRowID < readColIds.size())
                throw new IOException("Number of colfam:qual pairs + rowkey (" + (colQualFamPairs.size() + incForRowID) + ")" +
                        " numbers less than the hive table columns. (" + readColIds.size() + ")");

            Path[] tablePaths = FileInputFormat.getInputPaths(job);
            List<org.apache.hadoop.mapreduce.InputSplit> splits = super.getSplits(job); //get splits from Accumulo.
            InputSplit[] newSplits = new InputSplit[splits.size()];
            for (int i = 0; i < splits.size(); i++) {
                RangeInputSplit ris = (RangeInputSplit)splits.get(i);
                newSplits[i] = new AccumuloSplit(ris, tablePaths[0]);
            }
            return newSplits;
        }  catch (AccumuloException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (SerDeException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    private Instance getInstance(String id,
                                 String zookeepers) {
        if(instance != null) {
            return instance;
        } else {
            return new ZooKeeperInstance(id, zookeepers);
        }
    }

    //for testing purposes to set MockInstance
    public void setInstance(Instance instance) {
        this.instance = instance;
    }


    /**
     * Setup accumulo input format from conf properties.
     * Delegates to final RecordReader from mapred package.
     *
     * @param inputSplit
     * @param jobConf
     * @param reporter
     * @return RecordReader
     * @throws IOException
     */
    @Override
    public RecordReader<Text, AccumuloHiveRow> getRecordReader(
            InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException {


        String user = jobConf.get(AccumuloSerde.USER_NAME);
        String pass = jobConf.get(AccumuloSerde.USER_PASS);
        String id = jobConf.get(AccumuloSerde.INSTANCE_ID);
        String zookeepers = jobConf.get(AccumuloSerde.ZOOKEEPERS);
        instance = getInstance(id, zookeepers);
        AccumuloSplit as = (AccumuloSplit)inputSplit;
        RangeInputSplit ris = as.getSplit();
        Job job = new Job(jobConf);
        try {
            String colMapping = jobConf.get(AccumuloSerde.COLUMN_MAPPINGS);
            List<String> colQualFamPairs;
            colQualFamPairs = AccumuloHiveUtils.parseColumnMapping(colMapping);
            Connector connector = instance.getConnector(user, new PasswordToken(pass.getBytes()));
            configure(job, jobConf, connector, colQualFamPairs);

            List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);
            int incForRowID = AccumuloHiveUtils.containsRowID(colMapping) ? 1 : 0; //offset by +1 if table mapping contains rowID
            if (colQualFamPairs.size() + incForRowID < readColIds.size())
                throw new IOException("Number of colfam:qual pairs + rowID (" + (colQualFamPairs.size() + incForRowID) + ")" +
                        " numbers less than the hive table columns. (" + readColIds.size() + ")");


            //for use to initialize final record reader.
            final TaskAttemptContext tac =
                    new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()) {

                        @Override
                        public void progress() {
                            reporter.progress();;
                        }
                    };
            final org.apache.hadoop.mapreduce.RecordReader
                    <Text,PeekingIterator<Map.Entry<Key,Value>>> recordReader =
                    createRecordReader(ris, tac);
            recordReader.initialize(ris, tac);
            final int itrCount = getIterators(job).size();

            return new RecordReader<Text, AccumuloHiveRow>() {

                protected Text currentK;
                protected AccumuloHiveRow currentV;

                @Override
                public void close() throws IOException {
                    recordReader.close();
                }

                @Override
                public Text createKey() {
                    return new Text();
                }

                @Override
                public AccumuloHiveRow createValue() {
                    return new AccumuloHiveRow();
                }

                @Override
                public long getPos() throws IOException {
                    return 0;
                }

                @Override
                public float getProgress() throws IOException {
                    float progress = 0.0F;

                    try {
                        progress = recordReader.getProgress();
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }

                    return progress;
                }

                @Override
                public boolean next(Text rowKey, AccumuloHiveRow row) throws IOException {
                    boolean next;
                    try {
                        next = recordReader.nextKeyValue();
                        Text key = recordReader.getCurrentKey();
                        PeekingIterator<Map.Entry<Key,Value>> iter = recordReader.getCurrentValue();
                        if(next) {
                            row.clear();
                            row.setRowId(key.toString());
                            List<Key> keys = Lists.newArrayList();
                            List<Value> values = Lists.newArrayList();
                            while(iter.hasNext()) {  //collect key/values for this row.
                                Map.Entry<Key, Value> kv = iter.next();
                                keys.add(kv.getKey());
                                values.add(kv.getValue());

                            }
                            if(itrCount == 0) {  //no encoded values, we can push directly to row.
                                pushToValue(keys, values, row);
                            }
                            else {
                                for(int i = 0; i < itrCount; i++) { //each iterator creates a level of encoding.
                                    SortedMap<Key,Value> decoded =
                                            PrimitiveComparisonFilter.decodeRow(keys.get(0), values.get(0));
                                    keys = Lists.newArrayList(decoded.keySet());
                                    values = Lists.newArrayList(decoded.values());
                                }
                                pushToValue(keys, values, row); //after decoding we can push to value.
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new IOException(StringUtils.stringifyException(e));
                    }
                    return next;
                }

                //flatten key/value pairs into row object for use in Serde.
                private void pushToValue(List<Key> keys, List<Value> values, AccumuloHiveRow row)
                        throws IOException {
                    Iterator<Key> kIter = keys.iterator();
                    Iterator<Value> vIter = values.iterator();
                    while(kIter.hasNext()) {
                        Key k = kIter.next();
                        Value v = vIter.next();
                        byte[] utf8Val = AccumuloHiveUtils.valueAsUTF8bytes(jobConf, k, v);
                        row.add(k.getColumnFamily().toString(),
                                k.getColumnQualifier().toString(),utf8Val);
                    }
                }
            };

        } catch (AccumuloException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (InterruptedException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (SerDeException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    private void configure(Job job, JobConf conf, Connector connector, List<String> colQualFamPairs)
            throws AccumuloSecurityException, AccumuloException, SerDeException {
        String instanceId = job.getConfiguration().get(AccumuloSerde.INSTANCE_ID);
        String zookeepers = job.getConfiguration().get(AccumuloSerde.ZOOKEEPERS);
        String user = job.getConfiguration().get(AccumuloSerde.USER_NAME);
        String pass = job.getConfiguration().get(AccumuloSerde.USER_PASS);
        String tableName = job.getConfiguration().get(AccumuloSerde.TABLE_NAME);
        if (instance instanceof MockInstance) {
            setMockInstance(job, instanceId);
        }  else {
            setZooKeeperInstance(job, instanceId, zookeepers);
        }
        setConnectorInfo(job, user, new PasswordToken(pass.getBytes()));
        setInputTableName(job, tableName);
        setScanAuthorizations(job, connector.securityOperations().getUserAuthorizations(user));
        List<IteratorSetting> iterators = predicateHandler.getIterators(conf); //restrict with any filters found from WHERE predicates.
        for(IteratorSetting is : iterators)
            addIterator(job, is);
        Collection<Range> ranges = predicateHandler.getRanges(conf); //restrict with any ranges found from WHERE predicates.
        if(ranges.size() > 0)
            setRanges(job, ranges);
        fetchColumns(job, getPairCollection(colQualFamPairs));
    }

    /*
      Create col fam/qual pairs from pipe separated values, usually from config object. Ignores rowID.
     */
    private Collection<Pair<Text, Text>> getPairCollection(List<String> colQualFamPairs) {
        List<Pair<Text, Text>> pairs = Lists.newArrayList();
        for (String colQualFam : colQualFamPairs) {
            String[] qualFamPieces = PIPE.split(colQualFam);
            Text fam = new Text(qualFamPieces[0]);
            if(qualFamPieces.length > 1) {
                pairs.add(new Pair<Text, Text>(fam, new Text(qualFamPieces[1])));
            } else {
                pairs.add(new Pair<Text, Text>(fam, EMPTY_CQ));
            }
        }
        return pairs;
    }
}
