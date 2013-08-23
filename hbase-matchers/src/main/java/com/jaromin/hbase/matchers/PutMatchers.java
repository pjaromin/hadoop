package com.jaromin.hbase.matchers;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;
import static org.hamcrest.CoreMatchers.*;

/**;
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public class PutMatchers {

	public static T RowKeyMatcher<T> hasRowKey(Matcher<T> expected) {
		return new RowKeyMatcher<T>(expected);
	}
	
	public static RowKeyMatcher hasRowKey(byte[] expected) {
		return new RowKeyMatcher(expected, expected.toString());
	}
	
	public static RowKeyMatcher hasRowKey(Long expected) {
		return new RowKeyMatcher(Bytes.toBytes(expected), "" + expected);
	}
	
	public static RowKeyMatcher hasRowKey(Double expected) {
		return new RowKeyMatcher(Bytes.toBytes(expected), "" + expected);
	}
	
	public static RowKeyMatcher hasRowKey(Float expected) {
		return new RowKeyMatcher(Bytes.toBytes(expected), "" + expected);
	}

	public static RowKeyMatcher hasRowKey(Integer expected) {
		return new RowKeyMatcher(Bytes.toBytes(expected), "" + expected);
	}
	
	public static RowKeyMatcher hasRowKey(Short expected) {
		return new RowKeyMatcher(Bytes.toBytes(expected), "" + expected);
	}

	public static RowKeyMatcher hasRowKey(String expected) {
		return new RowKeyMatcher(Bytes.toBytes(expected), "" + expected);
	}
	
	public static <T> ColumnValueMatcher<T> hasColumnValue(String column, Matcher<T> matcher, Class<T> clazz) {
		String[] columnParts = StringUtils.split(column, ":");
		return new ColumnValueMatcher<T>(Bytes.toBytes(columnParts[0]), 
				Bytes.toBytes(columnParts[1]), matcher, clazz);
	}
	
	public static ColumnValueMatcher<byte[]> hasColumnValue(String column, Matcher<byte[]> matcher) {
		return hasColumnValue(column, matcher, byte[].class);
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
