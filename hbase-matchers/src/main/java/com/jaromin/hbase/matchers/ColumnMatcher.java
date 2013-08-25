package com.jaromin.hbase.matchers;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * Matches for the presence of a column in the specified {@link Put}
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class ColumnMatcher<T> extends FeatureMatcher<Put, T> {

	public static final String NAME = "Put Column Matcher";
	
	public static final String DESCRIPTION = "column family:qualifier";
	
	protected final byte[] columnFamilyBytes;
	
	protected final byte[] columnQualifierBytes;
	
	protected final Class<T> valueClass;

	/**
	 * 
	 * @param columnFamily
	 * @param columnQualifier
	 * @param subMatcher
	 * @param valueClass
	 */
	public ColumnMatcher(byte[] columnFamily, byte[] columnQualifier,
			Matcher<? super T> subMatcher, Class<T> valueClass) {
		this(columnFamily, columnQualifier, subMatcher, NAME, DESCRIPTION, valueClass);
	}
	
	protected ColumnMatcher(byte[] columnFamily, byte[] columnQualifier,
			Matcher<? super T> subMatcher, String name, String description, Class<T> valueClass) {
		super(subMatcher, name, description);
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
	@Override
	protected T featureValueOf(Put put) {
		List<KeyValue> list = put.get(this.columnFamilyBytes, this.columnQualifierBytes);
		if (CollectionUtils.isNotEmpty(list)) {
			KeyValue kv = list.get(0);
			byte[] keyBytes = kv.getKey();
			return (T)PutMatchers.valueOf(keyBytes, this.valueClass);
		}
		return null;
	}
	
//	public Matcher<U> withValue(Matcher<U> subMatcher, Class<U> valueClass) {
//		return new ColumnValueMatcher<U>(this.columnFamilyBytes, this.columnQualifierBytes, valueClass);
//	}
}
