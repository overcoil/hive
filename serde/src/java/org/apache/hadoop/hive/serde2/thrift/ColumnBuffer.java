package org.apache.hadoop.hive.serde2.thrift;

import java.nio.ByteBuffer;
import java.util.List;

/*
 * This will include most of the functionality of the column class.
 */
public class ColumnBuffer {
	
	private int size;
	private boolean[] boolVars;
	private byte[] byteVars;
	private short[] shortVars;
	private int[] intVars;
	private long[] longVars;
	private double[] doubleVars;
	private List<String> stringVars;
	private List<ByteBuffer> binaryVars;
	
	//private Type type; 
	
	
	
}
