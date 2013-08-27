package com.jaromin.hbase.matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
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
public class ColumnMatcher<T> extends TypeSafeDiagnosingMatcher<Mutation> {

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
	protected boolean matchesSafely(Mutation mutation, Description mismatch) {
		return findMatches(mutation, mismatch, true).size() > 0;
	}

	/**
	 * 
	 * @param mutation
	 * @param mismatch
	 * @param stopOnFirstMatch
	 * @return
	 */
	protected List<KeyValue> findMatches(Mutation mutation, Description mismatch, boolean stopOnFirstMatch) {
		List<KeyValue> matches = new ArrayList<KeyValue>();
		Map<byte[], List<KeyValue>> familyMap = mutation.getFamilyMap();
		int count = 0;
		String columnName;
		for (Entry<byte[], List<KeyValue>> family : familyMap.entrySet()) {
			// Family must be composed of printable characters
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

}
