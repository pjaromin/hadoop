package com.jaromin.hbase.matchers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

public class ColumnValueMatcherTest {

	Put put;
	
	byte[] rowKeyBytes = Bytes.toBytes("rowKey");
	
	byte[] familyBytes = Bytes.toBytes("a");
	
	List<KeyValue> columns;
	
	@Before
	public void setUp() {	
		put = new Put(rowKeyBytes);
		try {
			put.add(new KeyValue(rowKeyBytes, familyBytes, Bytes.toBytes("string"), 
					Bytes.toBytes("string_value")));
			put.add(new KeyValue(rowKeyBytes, familyBytes, Bytes.toBytes("long"), 
					Bytes.toBytes(100L)));
			put.add(new KeyValue(rowKeyBytes, familyBytes, Bytes.toBytes("double"), 
					Bytes.toBytes(100.1d)));
			put.add(new KeyValue(rowKeyBytes, familyBytes, Bytes.toBytes("int"), 
					Bytes.toBytes(100)));
			put.add(new KeyValue(rowKeyBytes, familyBytes, Bytes.toBytes("float"), 
					Bytes.toBytes(100.1f)));
			put.add(new KeyValue(rowKeyBytes, familyBytes, Bytes.toBytes("short"), 
					Bytes.toBytes((short)10)));
			put.add(new KeyValue(rowKeyBytes, familyBytes, Bytes.toBytes("bytes"), 
					Bytes.toBytes(100L)));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testMatchesPut_Bytes() {
		Matcher<byte[]> valueMatcher = CoreMatchers.is(Bytes.toBytes(100L));
		ColumnValueMatcher<byte[]> matcher 
			= PutMatchers.hasColumnValue("a:bytes", valueMatcher, byte[].class);
		assertThat(matcher.matches(this.put), is(true));
		
		matcher = PutMatchers.hasColumnValue("a:bytes", valueMatcher);
		assertThat(matcher.matches(this.put), is(true));
	}
	
	@Test
	public void testMatchesPut_String() {
		String colName = "a:string";
		String expectedValue ="string_value";
		Matcher<String> valueMatcher = CoreMatchers.is(expectedValue);
		ColumnValueMatcher<String> matcher 
			= PutMatchers.hasColumnValue(colName, valueMatcher, String.class);
		assertThat(matcher.matches(this.put), is(true));
		
		matcher = PutMatchers.hasStringColumnValue(colName, expectedValue);
		assertThat(matcher.matches(this.put), is(true));
	}

	@Test
	public void testMatchesPut_Long() {
		String colName = "a:long";
		Long expectedValue = 100L;
		Matcher<Long> valueMatcher = CoreMatchers.is(expectedValue);
		ColumnValueMatcher<Long> matcher 
			= PutMatchers.hasColumnValue(colName, valueMatcher, Long.class);
		assertThat(matcher.matches(this.put), is(true));
		
		matcher = PutMatchers.hasLongColumnValue(colName, expectedValue);
		assertThat(matcher.matches(this.put), is(true));
	}

	@Test
	public void testMatchesPut_Double() {
		String colName = "a:double";
		Double expectedValue = 100.1d;
		Matcher<Double> valueMatcher = CoreMatchers.is(expectedValue);
		ColumnValueMatcher<Double> matcher 
			= PutMatchers.hasColumnValue(colName, valueMatcher, Double.class);
		assertThat(matcher.matches(this.put), is(true));
		
		matcher = PutMatchers.hasDoubleColumnValue(colName, expectedValue);
		assertThat(matcher.matches(this.put), is(true));
	}

	@Test
	public void testMatchesPut_Float() {
		String colName = "a:float";
		Float expectedValue = 100.1f;
		Matcher<Float> valueMatcher = CoreMatchers.is(expectedValue);
		ColumnValueMatcher<Float> matcher 
			= PutMatchers.hasColumnValue(colName, valueMatcher, Float.class);
		assertThat(matcher.matches(this.put), is(true));
		
		matcher = PutMatchers.hasFloatColumnValue(colName, expectedValue);
		assertThat(matcher.matches(this.put), is(true));
	}
	
	@Test
	public void testMatchesPut_Integer() {
		Matcher<Integer> valueMatcher = CoreMatchers.is(100);
		ColumnValueMatcher<Integer> matcher 
			= PutMatchers.hasColumnValue("a:int", valueMatcher, Integer.class);
		assertThat(matcher.matches(this.put), is(true));
	}

	@Test
	public void testMatchesPut_Short() {
		Matcher<Short> valueMatcher = CoreMatchers.is((short)10);
		ColumnValueMatcher<Short> matcher 
			= PutMatchers.hasColumnValue("a:short", valueMatcher, Short.class);
		assertThat(matcher.matches(this.put), is(true));
	}
}
