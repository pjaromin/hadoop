package com.jaromin.hbase.matchers;

import static org.hamcrest.CoreMatchers.is;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;

/**;
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public abstract class PutMatchers {

	/**
	 * 
	 * @param <T>
	 * @param bytes
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T valueOf(byte[] bytes, Class<? extends T> valueClass) {
		if (byte[].class.equals(valueClass)) {
			return (T)bytes;
		} else if (String.class.equals(valueClass)) {
			return (T)Bytes.toString(bytes);
		}
		else if (Long.class.equals(valueClass)) {
			return (T)Long.valueOf(Bytes.toLong(bytes));
		}
		else if (Double.class.equals(valueClass)) {
			return (T)Double.valueOf(Bytes.toDouble(bytes));
		}
		else if (Float.class.equals(valueClass)) {
			return (T)Float.valueOf(Bytes.toFloat(bytes));
		}
		else if (Integer.class.equals(valueClass)) {
			return (T)Integer.valueOf(Bytes.toInt(bytes));
		}
		else if (Short.class.equals(valueClass)) {
			return (T)Short.valueOf(Bytes.toShort(bytes));
		}
		return null;
	}

	public static <T> RowKeyMatcher<T> hasRowKey(Matcher<T> expected, Class<T> typeClass) {
		return new RowKeyMatcher<T>(expected, typeClass);
	}

	public static RowKeyMatcher<byte[]> hasRowKey(byte[] expected) {
		return hasRowKey( is(expected), byte[].class);
	}
	
	public static RowKeyMatcher<String> hasRowKey(String expected) {
		return hasRowKey( is(expected), String.class);
	}
	
	public static RowKeyMatcher<String> hasRowKey(Matcher<String> expected) {
		return hasRowKey( is(expected), String.class);
	}
	
	public static RowKeyMatcher<Long> hasRowKey(Long expected) {
		return hasRowKey( is(expected), Long.class);
	}
	
	public static RowKeyMatcher<Double> hasRowKey(Double expected) {
		return hasRowKey( is(expected), Double.class);
	}
	
	public static RowKeyMatcher<Float> hasRowKey(Float expected) {
		return hasRowKey( is(expected), Float.class);
	}

	public static RowKeyMatcher<Integer> hasRowKey(Integer expected) {
		return hasRowKey( is(expected), Integer.class);
	}
	
	public static RowKeyMatcher<Short> hasRowKey(Short expected) {
		return hasRowKey( is(expected), Short.class);
	}
	
	public static <T> ColumnValueMatcher<T> hasColumnValue(String column, Matcher<T> matcher, Class<T> clazz) {
		String[] columnParts = StringUtils.split(column, ":");
		return new ColumnValueMatcher<T>(Bytes.toBytes(columnParts[0]), 
				Bytes.toBytes(columnParts[1]), matcher, clazz);
	}

	public static ColumnValueMatcher<String> hasColumnValue(String column, Matcher<String> matcher) {
		return hasColumnValue(column, matcher, String.class);
	}

	public static ColumnValueMatcher<byte[]> hasBytesColumnValue(String column, Matcher<byte[]> matcher) {
		return hasColumnValue(column, matcher, byte[].class);
	}

	public static ColumnValueMatcher<String> hasStringColumnValue(String column, Matcher<String> matcher) {
		return hasColumnValue(column, matcher, String.class);
	}

	public static ColumnValueMatcher<String> hasStringColumnValue(String column, String value) {
		return hasStringColumnValue(column, is(value));
	}
	
	public static ColumnValueMatcher<Long> hasLongColumnValue(String column, Matcher<Long> matcher) {
		return hasColumnValue(column, matcher, Long.class);
	}

	public static ColumnValueMatcher<Long> hasLongColumnValue(String column, Long value) {
		return hasLongColumnValue(column, is(value));
	}

	public static ColumnValueMatcher<Double> hasDoubleColumnValue(String column, Matcher<Double> matcher) {
		return hasColumnValue(column, matcher, Double.class);
	}

	public static ColumnValueMatcher<Double> hasDoubleColumnValue(String column, Double value) {
		return hasDoubleColumnValue(column, is(value));
	}
	
	public static ColumnValueMatcher<Float> hasFloatColumnValue(String column, Matcher<Float> matcher) {
		return hasColumnValue(column, matcher, Float.class);
	}

	public static ColumnValueMatcher<Float> hasFloatColumnValue(String column, Float value) {
		return hasFloatColumnValue(column, is(value));
	}
	
	public static ColumnValueMatcher<Integer> hasIntegerColumnValue(String column, Matcher<Integer> matcher) {
		return hasColumnValue(column, matcher, Integer.class);
	}

	public static ColumnValueMatcher<Integer> hasIntegerColumnValue(String column, Integer value) {
		return hasIntegerColumnValue(column, is(value));
	}

	public static ColumnValueMatcher<Short> hasShortColumnValue(String column, Matcher<Short> matcher) {
		return hasColumnValue(column, matcher, Short.class);
	}

	public static ColumnValueMatcher<Short> hasShortColumnValue(String column, Short value) {
		return hasShortColumnValue(column, is(value));
	}
}
