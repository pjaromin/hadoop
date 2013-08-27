package com.jaromin.hbase.matchers;

/**
 * Copyright 2013 Patrick K. Jaromin <patrick@jaromin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
