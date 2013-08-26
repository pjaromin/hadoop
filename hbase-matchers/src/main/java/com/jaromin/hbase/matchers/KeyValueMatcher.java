package com.jaromin.hbase.matchers;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import static org.hamcrest.Matchers.anything;

public class KeyValueMatcher<COL,VAL> extends TypeSafeDiagnosingMatcher<Put> {
	
	private final Matcher<Put> rowKeyMatcher;
	
	private ColumnMatcher<COL> columnMatcher;
	
	private final Matcher<VAL> valueMatcher;
	
	private final Class<VAL> valueClass;

	public KeyValueMatcher(Matcher<VAL> valueMatcher, Class<VAL> valueClass) {
		this(null, null, valueMatcher, valueClass);
	}

	public KeyValueMatcher(ColumnMatcher<COL> columnMatcher, 
			Matcher<VAL> valueMatcher, Class<VAL> valueClass) {
		this(null, columnMatcher, valueMatcher, valueClass);
	}
	
	public KeyValueMatcher(Matcher<Put> rowKeyMatcher, 
			ColumnMatcher<COL> columnMatcher, Matcher<VAL> valueMatcher, Class<VAL> valueClass) {
		this.rowKeyMatcher = rowKeyMatcher;
		this.columnMatcher = columnMatcher;
		this.valueMatcher = valueMatcher;
		this.valueClass = valueClass;
		
		if (this.columnMatcher == null) {
			// initialize with one that will match ANY
			this.columnMatcher = new ColumnMatcher<COL>((Matcher<COL>)anything(), null);
		}
	}
	
	@Override
	protected boolean matchesSafely(Put put, Description mismatch) {
		// First, verify the row matches if we have a matcher, otherwise
		// no need to continue...
		if (rowKeyMatcher != null && !rowKeyMatcher.matches(put)) {
			rowKeyMatcher.describeMismatch(put, mismatch);
			return false;
		}
		
		// Delegate check for column match to 
//		if (columnMatcher != null) {
			List<KeyValue> matchingKeyValues = columnMatcher.findMatches(put, mismatch, false);
			if (matchingKeyValues.size() == 0) {
				columnMatcher.describeMismatch(put, mismatch);
				return false;
			}

			// Check the key-values for a matching value
			int count = 0;
			for (KeyValue columnMatch : matchingKeyValues) {
				byte[] valueBytes = columnMatch.getValue();
				VAL value = (VAL)PutMatchers.valueOf(valueBytes, this.valueClass);
				if (valueMatcher.matches(value)){
					return true;
				}
				if (count++ > 0) {
	            	mismatch.appendText(", ");
	            }
	            valueMatcher.describeMismatch(value, mismatch);
			}
//		}
		return false;
	}

	@Override
	public void describeTo(Description mismatch) {
		mismatch.appendText("a column value matching ");
		valueMatcher.describeTo(mismatch);
	}

	/**
	 *  
	 * @param valueMatcher
	 * @return
	 */
	public static Matcher<Put> hasKeyValue(Matcher<String> valueMatcher) {
		return new KeyValueMatcher<String, String>(valueMatcher,String.class);
	}

	/**
	 * 
	 * @param columnMatcher
	 * @param valueMatcher
	 * @return
	 */
	public static Matcher<Put> hasKeyValue(ColumnMatcher<String> columnMatcher, Matcher<String> valueMatcher) {
		return new KeyValueMatcher<String, String>(columnMatcher,valueMatcher,String.class);
	}

	/**
	 * 
	 * @param rowKeyMatcher
	 * @param columnMatcher
	 * @param valueMatcher
	 * @return
	 */
	public static Matcher<Put> hasKeyValue(Matcher<Put> rowKeyMatcher, 
			ColumnMatcher<String> columnMatcher, Matcher<String> valueMatcher) {
		return new KeyValueMatcher<String, String>(rowKeyMatcher,
				columnMatcher,valueMatcher,String.class);
	}
}
