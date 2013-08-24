package com.jaromin.hbase.matchers;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.hamcrest.Matcher;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class ColumnValueMatcher<T> extends ColumnMatcher<T> {

	private static final String NAME = "";
	
	private static final String DESCRIPTION = "";
	
	/**
	 * 
	 * @param columnFamily
	 * @param columnQualifier
	 * @param subMatcher
	 * @param valueClass
	 */
	public ColumnValueMatcher(byte[] columnFamily, byte[] columnQualifier,
			Matcher<? super T> subMatcher, Class<T> valueClass) {
		super(columnFamily, columnQualifier, subMatcher, NAME, DESCRIPTION, valueClass);
	}

	/**
	 * Retrieves the value of the configured column 
	 * from the supplied <tt>Put</tt>.
	 * @param put
	 * @return
	 */
	@Override
	protected T featureValueOf(Put put) {
		List<KeyValue> list = put.get(this.columnFamilyBytes, this.columnQualifierBytes);
		if (CollectionUtils.isNotEmpty(list)) {
			KeyValue kv = list.get(0);
			byte[] valueBytes = kv.getValue();
			return (T)PutMatchers.valueOf(valueBytes, this.valueClass);
		}
		return null;
	}

}
