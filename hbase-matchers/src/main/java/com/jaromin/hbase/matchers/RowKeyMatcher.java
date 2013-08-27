package com.jaromin.hbase.matchers;

import org.apache.hadoop.hbase.client.Mutation;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public class RowKeyMatcher<T> extends FeatureMatcher<Mutation, T> {

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
	@Override
	protected T featureValueOf(Mutation mutation) {
		byte[] bytes = mutation.getRow();
		return (T)Matchers.valueOf(bytes, this.valueClass);
	}

}
