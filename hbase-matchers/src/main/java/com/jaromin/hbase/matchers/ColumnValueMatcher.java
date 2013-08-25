package com.jaromin.hbase.matchers;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class ColumnValueMatcher<T> extends TypeSafeDiagnosingMatcher<Put> {

	protected final byte[] columnFamily;
	
	protected final byte[] columnQualifier;
	
	protected final Class<T> valueClass;

	private final Matcher<? super T> subMatcher;
	
	/**
	 * 
	 * @param columnFamily
	 * @param columnQualifier
	 * @param subMatcher
	 * @param valueClass
	 */
	public ColumnValueMatcher(byte[] columnFamily, byte[] columnQualifier,
			Matcher<? super T> subMatcher, Class<T> valueClass) {
		this.columnFamily = columnFamily;
		this.columnQualifier = columnQualifier;
		this.valueClass = valueClass;
		this.subMatcher = subMatcher;
	}

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.TypeSafeDiagnosingMatcher#matchesSafely(java.lang.Object, org.hamcrest.Description)
	 */
	@Override
	protected boolean matchesSafely(Put put, Description mismatch) {
		List<KeyValue> kvList = put.get(this.columnFamily, this.columnQualifier);
		if (CollectionUtils.isNotEmpty(kvList)) {
			int count = 0;
			for (KeyValue kv : kvList) {
				byte[] valueBytes = kv.getValue();
				T value = (T)PutMatchers.valueOf(valueBytes, this.valueClass);
				if (subMatcher.matches(value)) {
					return true;
				}
	            if (count++ > 0) {
	            	mismatch.appendText(", ");
	            }
	            subMatcher.describeMismatch(value, mismatch);
			}
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	@Override
	public void describeTo(Description description) {
		description.appendText("has column value").appendText(" ")
        .appendDescriptionOf(subMatcher);
	}
}
