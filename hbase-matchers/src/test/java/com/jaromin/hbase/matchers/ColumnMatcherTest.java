package com.jaromin.hbase.matchers;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public class ColumnMatcherTest {

	private byte[] rowKey =  Bytes.toBytes("rowKey");

	private byte[] columnFamilyA = Bytes.toBytes("a");
	private byte[] columnFamilyB = Bytes.toBytes("b");

	@Test
	public void testHasStringColumn() {

		Put put = new Put(rowKey);
		put.add(columnFamilyA, "column1".getBytes(), "avalue".getBytes());
		put.add(columnFamilyA, "column2".getBytes(), "avalue".getBytes());
		put.add(columnFamilyA, "column3".getBytes(), "avalue".getBytes());
		put.add(columnFamilyB, "lastColumn".getBytes(), "avalue".getBytes());
		
		assertThat(put, Matchers.hasColumn("a:column1"));
		assertThat(put, Matchers.hasColumn(is("a:column1")));
		
		assertThat(put, Matchers.hasColumn(startsWith("a:col")));
		assertThat(put, Matchers.hasColumn(startsWith("b:last")));
		assertThat(put, Matchers.hasColumn(not(startsWith("c:col"))));
		assertThat(put, Matchers.hasColumn(not(startsWith("b:col"))));
		assertThat(put, Matchers.hasColumn(endsWith("column1")));
		assertThat(put, Matchers.hasColumn(containsString("last")));
		assertThat(put, Matchers.hasColumn(containsString("col")));
	}

	@Test
	public void testHasStringColumn_Delete() {

		Delete delete = new Delete(rowKey);
		delete.deleteColumn(columnFamilyA, "column1".getBytes());
		delete.deleteColumn(columnFamilyA, "column2".getBytes());
		delete.deleteColumn(columnFamilyA, "column3".getBytes());
		delete.deleteColumn(columnFamilyB, "lastColumn".getBytes());
		
		assertThat(delete, Matchers.hasColumn("a:column1"));
		assertThat(delete, Matchers.hasColumn(is("a:column1")));
		
		assertThat(delete, Matchers.hasColumn(startsWith("a:col")));
		assertThat(delete, Matchers.hasColumn(startsWith("b:last")));
		assertThat(delete, Matchers.hasColumn(not(startsWith("c:col"))));
		assertThat(delete, Matchers.hasColumn(not(startsWith("b:col"))));
		assertThat(delete, Matchers.hasColumn(endsWith("column1")));
		assertThat(delete, Matchers.hasColumn(containsString("last")));
		assertThat(delete, Matchers.hasColumn(containsString("col")));
	}
}
