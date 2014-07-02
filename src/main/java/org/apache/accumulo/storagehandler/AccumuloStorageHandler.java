package org.apache.accumulo.storagehandler;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.storagehandler.predicate.AccumuloPredicateHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Create table mapping to Accumulo for Hive.
 * Handle predicate pushdown if necessary.
 */
public class AccumuloStorageHandler
        implements HiveStorageHandler,HiveMetaHook, HiveStoragePredicateHandler {
    private Configuration conf;
    private Connector connector;

    private static final Logger log = Logger.getLogger(AccumuloStorageHandler.class);
    static {
        log.setLevel(Level.INFO);
    }
    private AccumuloPredicateHandler predicateHandler = AccumuloPredicateHandler.getInstance();

    private Connector getConnector()
            throws MetaException{

        if (connector == null){
            try {
                connector = AccumuloHiveUtils.getConnector(conf);
            } catch (IOException e) {
                throw new MetaException(StringUtils.stringifyException(e));
            }
        }
        return connector;
    }

    /**
     *
     * @param desc table description
     * @param jobProps
     */
    @Override
    public void configureTableJobProperties(TableDesc desc,
                                            Map<String, String> jobProps) {
        Properties tblProperties = desc.getProperties();
        jobProps.put(AccumuloSerde.COLUMN_MAPPINGS,
                tblProperties.getProperty(AccumuloSerde.COLUMN_MAPPINGS));
        String tableName = tblProperties.getProperty(AccumuloSerde.TABLE_NAME);
        jobProps.put(AccumuloSerde.TABLE_NAME, tableName);
        String useIterators = tblProperties.getProperty(AccumuloSerde.NO_ITERATOR_PUSHDOWN);
        if(useIterators != null) {
            jobProps.put(AccumuloSerde.NO_ITERATOR_PUSHDOWN, useIterators);
        }

    }

    @Override
    public void configureJobConf(TableDesc desc, JobConf jobConf) {
        Properties tblProperties = desc.getProperties();
        jobConf.set(AccumuloSerde.COLUMN_MAPPINGS,
            tblProperties.getProperty(AccumuloSerde.COLUMN_MAPPINGS));
        String tableName = tblProperties.getProperty(AccumuloSerde.TABLE_NAME);
        jobConf.set(AccumuloSerde.TABLE_NAME, tableName);
        String useIterators = tblProperties.getProperty(AccumuloSerde.NO_ITERATOR_PUSHDOWN);
        if(useIterators != null) {
          jobConf.set(AccumuloSerde.NO_ITERATOR_PUSHDOWN, useIterators);
        }
    }

    private String getTableName(Table table) throws MetaException{
        String tableName = table.getSd().getSerdeInfo().getParameters().get(AccumuloSerde.TABLE_NAME);
        if (tableName == null)   {
            throw new MetaException("Please specify " + AccumuloSerde.TABLE_NAME + " in TBLPROPERTIES");
        }
        return tableName;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return AccumuloSerde.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> properties) {
        Properties props = tableDesc.getProperties();
        properties.put(AccumuloSerde.COLUMN_MAPPINGS,
                props.getProperty(AccumuloSerde.COLUMN_MAPPINGS));
        properties.put(AccumuloSerde.TABLE_NAME,
                props.getProperty(AccumuloSerde.TABLE_NAME));
        String useIterators = props.getProperty(AccumuloSerde.NO_ITERATOR_PUSHDOWN);
        if(useIterators != null) {
            properties.put(AccumuloSerde.NO_ITERATOR_PUSHDOWN, useIterators);
        }
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> properties) {
      Properties props = tableDesc.getProperties();
      properties.put(AccumuloSerde.COLUMN_MAPPINGS,
              props.getProperty(AccumuloSerde.COLUMN_MAPPINGS));
      properties.put(AccumuloSerde.TABLE_NAME,
              props.getProperty(AccumuloSerde.TABLE_NAME));
      String useIterators = props.getProperty(AccumuloSerde.NO_ITERATOR_PUSHDOWN);
      if(useIterators != null) {
          properties.put(AccumuloSerde.NO_ITERATOR_PUSHDOWN, useIterators);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends InputFormat> getInputFormatClass() {
        return HiveAccumuloTableInputFormat.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveAccumuloTableOutputFormat.class;
    }


    @Override
    public void preCreateTable(Table table) throws MetaException {
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        if (table.getSd().getLocation() != null){
            throw new MetaException("Location can't be specified for Accumulo");
        }
        try {
            String tblName = getTableName(table);
            Connector connector = getConnector();
            TableOperations tableOpts = connector.tableOperations();
            Map<String, String> serdeParams = table.getSd().getSerdeInfo().getParameters();
            String columnMapping = serdeParams.get(AccumuloSerde.COLUMN_MAPPINGS);
            if (columnMapping == null)
                throw new MetaException(AccumuloSerde.COLUMN_MAPPINGS + " missing from SERDEPROPERTIES");
            if (!tableOpts.exists(tblName)) {
                if(!isExternal) {
                    tableOpts.create(tblName);
                    tableOpts.online(tblName);
                } else {
                    throw new MetaException("Accumulo table " + tblName + " doesn't exist even though declared external");
                }
            } else {
                if (!isExternal) {
                    throw new MetaException("Table " + tblName + " already exists. Use CREATE EXTERNAL TABLE to register with Hive.");
                }
            }

        } catch (AccumuloSecurityException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableExistsException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (AccumuloException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (IllegalArgumentException e) {
            log.info("Error parsing column mapping");
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        String tblName = getTableName(table);
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        try {
            TableOperations tblOpts = getConnector().tableOperations();
            if(!isExternal && tblOpts.exists(tblName)){
                tblOpts.delete(tblName);
            }
        } catch (AccumuloException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {
        //do nothing
    }

    @Override
    public void commitDropTable(Table table, boolean deleteData) throws MetaException {
        String tblName = getTableName(table);
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        try {
            if(!isExternal && deleteData){
                TableOperations tblOpts = getConnector().tableOperations();
                if(tblOpts.exists(tblName))
                    tblOpts.delete(tblName);
            }
        } catch (AccumuloException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public void preDropTable(Table table) throws MetaException {
        //do nothing
    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {
        //do nothing
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf conf,
                                                  Deserializer deserializer,
                                                  ExprNodeDesc desc) {
        if(conf.get(AccumuloSerde.NO_ITERATOR_PUSHDOWN) == null){
            return predicateHandler.decompose(conf, desc);
        } else {
            log.info("Set to ignore iterator. skipping predicate handler");
            return null;
        }
    }
}
