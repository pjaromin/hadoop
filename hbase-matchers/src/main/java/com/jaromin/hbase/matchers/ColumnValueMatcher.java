package com.jaromin.hbase.matchers;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class ColumnValueMatcher<T> extends TypeSafeMatcher<Put> {

	private final byte[] columnFamilyBytes;
	
	private final byte[] columnQualifierBytes;

	private final Matcher<T> expectedValueMatcher;
	
	private final Class<T> valueClass;
	
	public ColumnValueMatcher(byte[] columnFamily, byte[] columnQualifier,
			Matcher<T> expectedValue, Class<T> valueClass) {
		this.columnFamilyBytes = columnFamily;
		this.columnQualifierBytes = columnQualifier;
		this.expectedValueMatcher = expectedValue;
		this.valueClass = valueClass;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.TypeSafeMatcher#matchesSafely(java.lang.Object)
	 */
	@Override
	protected boolean matchesSafely(Put put) {
		T value = getColumnValue(put);
		return value != null 
				&& expectedValueMatcher.matches(value);
	}

	/**
	 * Retrieves the value of the configured column 
	 * from the supplied <tt>Put</tt>.
	 * @param put
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected T getColumnValue(Put put) {
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

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	@Override
	public void describeTo(Description description) {
		description.appendText("Expected ").appendValue(expectedValueMatcher);
	}
}
