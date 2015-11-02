package org.apache.hadoop.hive.serde2.thrift;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractEncodingAwareSerDe;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;


@SuppressWarnings("deprecation")

@SerDeSpec(schemaProps = {
	    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
	    })
public class ThriftExecSerDe extends AbstractSerDe {

	
	public static final Log LOG = LogFactory.getLog(ThriftExecSerDe.class.getName());
	
	public ThriftExecSerDe() throws SerDeException {
		super();
	}

	//private StructTypeInfo rowTypeInfo;
	private StructObjectInspector rowObjectInspector;
	private List<String> columnNames;//we can get this from the tbl properties
	private List<TypeInfo> columnTypes;
	private List<Object> row;
	private int numColumns; 
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.Deserializer#initialize(org.apache.hadoop.conf.Configuration, java.util.Properties)
	 * This will use the table properties to get a list of column names and types. and create objects to be used later. 
	 * Remember that our aim is to deserialize what we get into thrift objects. To do so, we will need a list of column names 
	 * and their types.  
	 */
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {
		
		String cNames = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		columnNames = Arrays.asList(cNames.split(","));
		
		String cTypes = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(cTypes);
		
		numColumns = columnNames.size();
		
		//do i have to handle the case when columns aren't primitive? i can't do much about it, so there's one reason to.
		List<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(columnNames.size());
		for (int c = 0; c < numColumns; c++) {
			columnObjectInspectors.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(columnTypes.get(c)));
		}
		rowObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnObjectInspectors);
		//reuse
		row = new ArrayList<Object>(numColumns);
		for (int c = 0; c < numColumns; c++) {
			row.add(null);
		}
	}

	public Object doDeserialize(Writable blob) throws SerDeException {
		List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
		List<Object> list = rowObjectInspector.getStructFieldsDataAsList(blob);
		//next step will be to get the right objectInspector for each and handle the primitive categories depending on type 
		return null;
	}

	public ObjectInspector getObjectInspector() throws SerDeException {
		
		return null;
	}

	public SerDeStats getSerDeStats() {
		
		return null;
	}

	public Class<? extends Writable> getSerializedClass() {
		
		return null;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}



	

}
