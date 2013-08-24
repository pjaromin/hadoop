package com.jaromin.hbase.matchers;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public class RowKeyMatcher<T> extends FeatureMatcher<Put, T> {

	public static final String NAME = "Put Row Key";
	
	public static final String DESCRIPTION = "row key";

	private final Class<T> valueClass;

	public RowKeyMatcher(Matcher<? super T> subMatcher, Class<T> valueClass) {
		super(subMatcher, NAME, DESCRIPTION);
		this.valueClass = valueClass;
	}

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.FeatureMatcher#featureValueOf(java.lang.Object)
	 */
	@Override @SuppressWarnings("unchecked")
	protected T featureValueOf(Put put) {
		byte[] bytes = put.getRow();
		if (byte[].class.equals(this.valueClass)) {
			return (T)bytes;
		} else if (String.class.equals(this.valueClass)) {
			return (T)Bytes.toString(bytes);
		}
		else if (Long.class.equals(this.valueClass)) {
			return (T)Long.valueOf(Bytes.toLong(bytes));
		}
		else if (Double.class.equals(this.valueClass)) {
			return (T)Double.valueOf(Bytes.toDouble(bytes));
		}
		else if (Float.class.equals(this.valueClass)) {
			return (T)Float.valueOf(Bytes.toFloat(bytes));
		}
		else if (Integer.class.equals(this.valueClass)) {
			return (T)Integer.valueOf(Bytes.toInt(bytes));
		}
		else if (Short.class.equals(this.valueClass)) {
			return (T)Short.valueOf(Bytes.toShort(bytes));
		}
		return null;
	}

}
