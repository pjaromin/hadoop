package com.jaromin.hbase.matchers;

import org.apache.hadoop.hbase.client.Put;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public class RowKeyMatcher extends TypeSafeMatcher<Put> {
	
	private final byte[] expected;
	
	private final String describeValue;
	
	/**
	 * 
	 * @param expected
	 * @param describeValue
	 */
	public RowKeyMatcher(byte[] expected, String describeValue) {
		this.expected = expected;
		this.describeValue = describeValue;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.TypeSafeMatcher#matchesSafely(java.lang.Object)
	 */
	@Override
	protected boolean matchesSafely(Put put) {
		return put.getRow() == this.expected;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	@Override
	public void describeTo(Description description) {
		description.appendText("Expected row key of ")
				.appendValue(this.describeValue);
	}

}
