package com.jaromin.hbase.matchers;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class ColumnValueMatcher<T> extends FeatureMatcher<Put, T> {

	private static final String NAME = "";
	
	private static final String DESCRIPTION = "";

	private final byte[] columnFamilyBytes;
	
	private final byte[] columnQualifierBytes;
	
	private final Class<T> valueClass;
	
	public ColumnValueMatcher(byte[] columnFamily, byte[] columnQualifier,
			Matcher<? super T> subMatcher, Class<T> valueClass) {
		super(subMatcher, NAME, DESCRIPTION);
		this.columnFamilyBytes = columnFamily;
		this.columnQualifierBytes = columnQualifier;
		this.valueClass = valueClass;
	}

	/**
	 * Retrieves the value of the configured column 
	 * from the supplied <tt>Put</tt>.
	 * @param put
	 * @return
	 */
	@Override @SuppressWarnings("unchecked")
	protected T featureValueOf(Put put) {
		List<KeyValue> list = put.get(this.columnFamilyBytes, this.columnQualifierBytes);
		if (CollectionUtils.isNotEmpty(list)) {
			KeyValue kv = list.get(0);
			byte[] valueBytes = kv.getValue();
			if (byte[].class.equals(this.valueClass)) {
				return (T)valueBytes;
			} else if (String.class.equals(this.valueClass)) {
				return (T)Bytes.toString(valueBytes);
			}
			else if (Long.class.equals(this.valueClass)) {
				return (T)Long.valueOf(Bytes.toLong(valueBytes));
			}
			else if (Double.class.equals(this.valueClass)) {
				return (T)Double.valueOf(Bytes.toDouble(valueBytes));
			}
			else if (Float.class.equals(this.valueClass)) {
				return (T)Float.valueOf(Bytes.toFloat(valueBytes));
			}
			else if (Integer.class.equals(this.valueClass)) {
				return (T)Integer.valueOf(Bytes.toInt(valueBytes));
			}
			else if (Short.class.equals(this.valueClass)) {
				return (T)Short.valueOf(Bytes.toShort(valueBytes));
			}
		}
		return null;
	}

}
