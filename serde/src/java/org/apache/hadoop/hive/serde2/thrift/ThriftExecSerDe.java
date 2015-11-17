package org.apache.hadoop.hive.serde2.thrift;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Writable;

public class ThriftExecSerDe extends AbstractSerDe {

	private StructTypeInfo rowTypeInfo;
	private ObjectInspector rowObjectInspector;
	private List<ColumnBuffer> columnBuffers;
	private List<String> columnNames;
	
	@Override
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {
		String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		columnNames = Arrays.asList(colNamesStr.split(","));
		columnBuffers = new ArrayList<ColumnBuffer>(columnNames.size());//so now, we are creating n buffers, where n is size of column.
		String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		
		
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return null;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		//what are we doing here? find all it's fields, for each field, parse it. when i say parse, we 
		return null;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
