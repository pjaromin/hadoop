package com.jaromin.hbase.matchers;

import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Matches for the presence of a column in the specified {@link Put}
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class ColumnMatcher<T> extends TypeSafeDiagnosingMatcher<Put> {

	private Matcher<? super T> nameMatcher;
	
	private Class<T> nameTypeClass;
	
	/**
	 * 
	 * @param nameMatcher
	 * @param nameTypeClass
	 */
	public ColumnMatcher(Matcher<T> nameMatcher, Class<T> nameTypeClass) {
		this.nameMatcher = nameMatcher;
		this.nameTypeClass = nameTypeClass;
	}

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.TypeSafeDiagnosingMatcher#matchesSafely(java.lang.Object, org.hamcrest.Description)
	 */
	@Override
	protected boolean matchesSafely(Put put, Description mismatch) {
		return findMatches(put, mismatch, true).size() > 0;
	}

	/**
	 * 
	 * @param put
	 * @param mismatch
	 * @param stopOnFirstMatch
	 * @return
	 */
	protected List<KeyValue> findMatches(Put put, Description mismatch, boolean stopOnFirstMatch) {
		List<KeyValue> matches = new ArrayList<KeyValue>();
		Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
		int count = 0;
		String columnName;
		for (Entry<byte[], List<KeyValue>> family : familyMap.entrySet()) {
			String familyStr = Bytes.toString(family.getKey());
			for (KeyValue column : family.getValue()) {
				String qualifier = Bytes.toString(column.getQualifier());
				// Match the name using the supplied matcher.
				columnName = familyStr + ":" + qualifier;
				if (this.nameMatcher.matches(columnName)) {
					matches.add(column);
					if (stopOnFirstMatch) {
						return matches;
					}
				}
				if (count++ > 0) {
					mismatch.appendText(", ");
				}
				nameMatcher.describeMismatch(columnName, mismatch);
			}
		}
		return matches;
	}
	
	@Override
	public void describeTo(Description mismatch) {
		mismatch.appendText("a column name matching ");
		nameMatcher.describeTo(mismatch);
	}

	public static ColumnMatcher<String> column(Matcher<String> matcher) {
		return new ColumnMatcher<String>(matcher, String.class);
	}

	public static ColumnMatcher<String> column(String string) {
		return column(is(string));
	}

	public static ColumnMatcher<Long> columnLong(Matcher<Long> matcher) {
		return new ColumnMatcher<Long>(matcher, Long.class);
	}

	public static ColumnMatcher<Long> columnLong(Long value) {
		return columnLong(is(value));
	}

	public static ColumnMatcher<Double> columnDouble(Matcher<Double> matcher) {
		return new ColumnMatcher<Double>(matcher, Double.class);
	}

	public static ColumnMatcher<Double> columnDouble(Double Double) {
		return columnDouble(is(Double));
	}
	
	public static ColumnMatcher<Float> columnFloat(Matcher<Float> matcher) {
		return new ColumnMatcher<Float>(matcher, Float.class);
	}

	public static ColumnMatcher<Float> columnFloat(Float Float) {
		return columnFloat(is(Float));
	}
	
	public static ColumnMatcher<Integer> columnInteger(Matcher<Integer> matcher) {
		return new ColumnMatcher<Integer>(matcher, Integer.class);
	}

	public static ColumnMatcher<Integer> columnInteger(Integer Integer) {
		return columnInteger(is(Integer));
	}
	
	public static ColumnMatcher<Short> columnShort(Matcher<Short> matcher) {
		return new ColumnMatcher<Short>(matcher, Short.class);
	}

	public static ColumnMatcher<Short> columnShort(Short value) {
		return columnShort(is(value));
	}
	
	public static ColumnMatcher<byte[]> columnBytes(Matcher<byte[]> matcher) {
		return new ColumnMatcher<byte[]>(matcher, byte[].class);
	}

	public static ColumnMatcher<byte[]> columnBytes(byte[] bytes) {
		return columnBytes(is(bytes));
	}
}
