package com.jaromin.mapreduce.io;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

public class CompositeSortKeyTextIntTest {

	private TestMapper mapper;
	private TestReducer reducer;
	private MapReduceDriver<LongWritable, Text, CompositeSortKey<Text,IntWritable>, Text, Text, NullWritable> driver;

	@Test
	public void testNaturalSortByValue() {
		// Add in a non-sorted order
		int offset = 50;
		setupInputs(offset);
		
		// Outputs should be properly sorted...
		// First 'A'
		for (int i = 0; i < offset*3; i++) {
			driver.addOutput(new Text("A" + i), NullWritable.get());
		}
		// And 'B'
		for (int i = 0; i < offset*3; i++) {
			driver.addOutput(new Text("B" + i), NullWritable.get());
		}

		try {
			driver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test @SuppressWarnings("unchecked")
	public void testReverseSortByValue() {
		// Add in a non-sorted order
		int offset = 50;
		setupInputs(offset);
		
		// Outputs should be properly sorted...
		// First 'A'
		for (int i = offset*3-1; i > -1; i--) {
			driver.addOutput(new Text("A" + i), NullWritable.get());
		}
		// And 'B'
		for (int i = offset*3-1; i > -1; i--) {
			driver.addOutput(new Text("B" + i), NullWritable.get());
		}

		try {
			driver.setKeyOrderComparator(new CompositeSortKey.ReverseSortComparator());
			driver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void setupInputs(int offset) {
		for (int i = 0; i < offset; i++) {
			int i2 = i+offset;
			int i3 = i+(offset*2);
			driver.addInput(new LongWritable(i3), new Text("B," + i3 + ",B" + i3));
			driver.addInput(new LongWritable(i), new Text("B," + i + ",B" + i));
			driver.addInput(new LongWritable(i2), new Text("A," + i2 + ",A" + i2));
			driver.addInput(new LongWritable(i), new Text("A," + i + ",A" + i));
			driver.addInput(new LongWritable(i3), new Text("A," + i3 + ",A" + i3));
			driver.addInput(new LongWritable(i2), new Text("B," + i2 + ",B" + i2));
		}
	}

	@Before @SuppressWarnings("unchecked")
	public void setup() {
		mapper = new TestMapper();
		reducer = new TestReducer();
		driver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
		Configuration conf = driver.getConfiguration();
		Job job = mock(Job.class);
		when(job.getConfiguration()).thenReturn(conf);

		CompositeSortKeySerialization.configureMapOutputKey(job, Text.class, IntWritable.class);
		
		// MRUnit sets these differently than standard MapReduce:
		driver.setKeyGroupingComparator(new CompositeSortKey.GroupingComparator());
	}
	
	private static final class TestMapper extends Mapper<LongWritable, Text, CompositeSortKey<Text,IntWritable>, Text> {

		private final CompositeSortKey<Text, IntWritable> KEY = new CompositeSortKey<Text, IntWritable>();
		private final Text VALUE = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data = StringUtils.split(value.toString());
			KEY.setGroupKey(new Text(data[0]));
			KEY.setSortKey(new IntWritable(Integer.valueOf(data[1])));
			VALUE.set(data[2]);
			context.write(KEY, VALUE);
		}
		
	}

	private static final class TestReducer extends Reducer<CompositeSortKey<Text,IntWritable>, Text, Text, NullWritable> {

		@Override
		protected void reduce(CompositeSortKey<Text,IntWritable> key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("KEY: " + key.toString());
			for (Text value : values) {
				System.out.println(value);
				context.write(new Text(value), NullWritable.get());
			}
		}
		
	}
}
