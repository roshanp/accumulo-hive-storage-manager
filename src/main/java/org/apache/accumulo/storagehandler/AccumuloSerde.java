package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Deserialization from Accumulo to LazyAccumuloRow for Hive.
 *
 */

public class AccumuloSerde implements SerDe {
    public static final String TABLE_NAME = "accumulo.table.name";
    public static final String USER_NAME = "accumulo.user.name";
    public static final String USER_PASS = "accumulo.user.pass";
    public static final String ZOOKEEPERS = "accumulo.zookeepers";
    public static final String INSTANCE_ID = "accumulo.instance.id";
    public static final String COLUMN_MAPPINGS = "accumulo.columns.mapping";
    public static final String NO_ITERATOR_PUSHDOWN = "accumulo.no.iterators";
    private static final String MORE_ACCUMULO_THAN_HIVE = "You have more " + COLUMN_MAPPINGS + " fields than hive columns";
    private static final String MORE_HIVE_THAN_ACCUMULO = "You have more hive columns than fields mapped with " + COLUMN_MAPPINGS;
    private LazySimpleSerDe.SerDeParameters serDeParameters;
    private LazyAccumuloRow cachedRow;
    private List<String> fetchCols;
    private ObjectInspector cachedObjectInspector;
    private AccumuloHiveRow row;
    private Configuration jobConfiguration;
    private static final Pattern PIPE = Pattern.compile("[|]");

    private static final Logger log = Logger.getLogger(AccumuloSerde.class);
    static {
        log.setLevel(Level.INFO);
    }

    public void initialize(Configuration conf, Properties properties) throws SerDeException {
        row = new AccumuloHiveRow();
        initAccumuloSerdeParameters(conf, properties);

        cachedObjectInspector = LazyFactory.createLazyStructInspector(
                serDeParameters.getColumnNames(),
                serDeParameters.getColumnTypes(),
                serDeParameters.getSeparators(),
                serDeParameters.getNullSequence(),
                serDeParameters.isLastColumnTakesRest(),
                serDeParameters.isEscaped(),
                serDeParameters.getEscapeChar());

        cachedRow = new LazyAccumuloRow((LazySimpleStructObjectInspector) cachedObjectInspector);

        if(log.isInfoEnabled()) {
            log.info("Initialized with " + serDeParameters.getColumnNames() +
                    " type: " + serDeParameters.getColumnTypes());
        }
    }

    /***
     * For testing purposes.
     */
    public LazyAccumuloRow getCachedRow() {
        return cachedRow;
    }

    private void initAccumuloSerdeParameters(Configuration conf, Properties properties)
            throws SerDeException{
        String colMapping = properties.getProperty(COLUMN_MAPPINGS);
        String colTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        String name = getClass().getName();
        fetchCols = AccumuloHiveUtils.parseColumnMapping(colMapping);
        if (colTypeProperty == null) {
            StringBuilder builder = new StringBuilder();
            for (String fetchCol : fetchCols) { //default to all string if no column type property.
                builder.append(serdeConstants.STRING_TYPE_NAME + ":");
            }
            int indexOfLastColon = builder.lastIndexOf(":");
            builder.replace(indexOfLastColon, indexOfLastColon+1, "");
            properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, builder.toString());
        }

        serDeParameters = LazySimpleSerDe.initSerdeParams(conf, properties, name);
        if (fetchCols.size() != serDeParameters.getColumnNames().size()) {
            throw new SerDeException(name + ": Hive table definition has "
                    + serDeParameters.getColumnNames().size() +
                    " elements while " + COLUMN_MAPPINGS + " has " +
                    fetchCols.size() + " elements. " + printColumnMismatchTip(fetchCols.size(),
                    serDeParameters.getColumnNames().size()));
        }

        if(log.isInfoEnabled())
            log.info("Serde initialized successfully for column mapping: " + colMapping);
    }


    private String printColumnMismatchTip(int accumuloColumns, int hiveColumns) {

        if(accumuloColumns < hiveColumns) {
            return MORE_HIVE_THAN_ACCUMULO;
        } else {
            return MORE_ACCUMULO_THAN_HIVE;
        }
    }

    public Class<? extends Writable> getSerializedClass() {
        return Mutation.class;
    }

    /***
     * convert Object into an AccumuloHiveRow (Writable) which is used by HiveAccumuloTableOutputFormat
     */
    public Writable serialize(Object o, ObjectInspector objectInspector)
            throws SerDeException {
        // clear any existing data in AccumuloHiveRow
        row.clear();
        StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;
        List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
        // count of columns SerDe was initialized with
        int fieldCount = serDeParameters.getColumnNames().size();
        log.info("fetchCols (accumulo.columns.mapping) = " + fetchCols.toString());
        // loop for each hive field that SerDe was initialized with and lookup values in Object o to convert to AccumuloHiveRow
        for(int i=0; i < fieldCount; i++){
          log.info("hive column = " + serDeParameters.getColumnNames().get(i));
          StructField structField = fields.get(i);
          String accumuloCol = fetchCols.get(i);
          if (structField != null){
            // Object has this column
            // lookup accumulo mapping for this hive column and transform to AccumuloHiveRow
            Object fieldData = structObjectInspector.getStructFieldData(o, structField);
            ObjectInspector fieldDataOI = fields.get(i).getFieldObjectInspector();
            log.info("accumulo column fam/qual " + accumuloCol);
            log.info("column type " + serDeParameters.getColumnTypes().get(i).getTypeName());
            StringObjectInspector fieldDataStringOI = (StringObjectInspector)fieldDataOI;
            Text value = fieldDataStringOI.getPrimitiveWritableObject(fieldData);
            log.info("column value "  + value.toString());
            if(accumuloCol.equals("rowID")){
              row.setRowId(value.toString());
            } else {
              if(value != null && value.equals("")){
                // split column family and column qualifier
                String[] line = PIPE.split(accumuloCol);
                log.info("line " + line.toString() + line.length);   
                String columnFamily = line[0];
                String columnQualifier = line[1];
                log.info("columnFamily = " + columnFamily + "  columnQualifier = " + columnQualifier);
                row.add(columnFamily, columnQualifier, value.getBytes());
              }

            }
          }
        }
        log.info("returning row = " + row.toString());
        return row;
        //throw new UnsupportedOperationException("Serialization to Accumulo not yet supported" + fetchCols.toString());
    }

    public Object deserialize(Writable writable) throws SerDeException {
        if(!(writable instanceof AccumuloHiveRow)) {
            throw new SerDeException(getClass().getName() + " : " +
                    "Expected AccumuloHiveRow. Got " + writable.getClass().getName());
        }

        cachedRow.init((AccumuloHiveRow)writable, fetchCols);
        return cachedRow;
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return cachedObjectInspector;
    }

    public SerDeStats getSerDeStats() {
        throw new UnsupportedOperationException("SerdeStats not supported.");
    }
}
