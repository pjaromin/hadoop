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

import static org.hamcrest.Matchers.anything;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class KeyValueMatcher<COL,VAL> extends TypeSafeDiagnosingMatcher<Put> {

	private ColumnMatcher<COL> columnMatcher;
	
	private final Matcher<VAL> valueMatcher;
	
	private final Class<VAL> valueClass;

	public KeyValueMatcher(Matcher<VAL> valueMatcher, Class<VAL> valueClass) {
		this(null, valueMatcher, valueClass);
	}

	@SuppressWarnings("unchecked")
	public KeyValueMatcher(ColumnMatcher<COL> columnMatcher, Matcher<VAL> valueMatcher,
			Class<VAL> valueClass) {
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
		// Delegate check for column match to 
		List<KeyValue> matchingKeyValues = columnMatcher.findMatches(put, mismatch, false);
		if (matchingKeyValues.size() == 0) {
			columnMatcher.describeMismatch(put, mismatch);
			return false;
		}

		// Check the key-values for a matching value
		int count = 0;
		for (KeyValue columnMatch : matchingKeyValues) {
			byte[] valueBytes = columnMatch.getValue();
			VAL value = (VAL)Matchers.valueOf(valueBytes, this.valueClass);
			if (valueMatcher.matches(value)){
				return true;
			}
			if (count++ > 0) {
            	mismatch.appendText(", ");
            }
            valueMatcher.describeMismatch(value, mismatch);
		}
		return false;
	}

	@Override
	public void describeTo(Description mismatch) {
		mismatch.appendText("a column value matching ");
		valueMatcher.describeTo(mismatch);
	}

}
