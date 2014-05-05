/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package Tool;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;

/**
 * Import data written by {@link Export}.
 */
public class LoadTable {
	final static String NAME = "import";
	public static final String CF_RENAME_PROP = "HBASE_IMPORTER_RENAME_CFS";

	public static byte[] family = "f1".getBytes();
	public static byte[] qualifier = "c1".getBytes();
	
	
	public static class ImportMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		String tableName;
		@Override
		  protected void setup(Context context) {

		    Configuration conf = context.getConfiguration();
		    tableName = conf.get(TableOutputFormat.OUTPUT_TABLE);
		  }
		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException {
			String tmp  = value.toString();
			String array[] = tmp.split("\\|");
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(array[0]));
			String valueStr = "";
			int maxPos = array.length;
			if(tableName.equals("part"))
				maxPos = 8; 
			else
				maxPos = 7;
			for(int i = 1; i < maxPos - 1; i ++){
				valueStr += array[i] + "|"; 
			}
			valueStr += array[maxPos - 1];
			
			Put put = new Put(rowKey.copyBytes());
			put.add(family, qualifier, valueStr.getBytes());
			try {
				context.write(rowKey, put);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static Job createSubmittableJob(Configuration conf, String[] args)
			throws IOException {

		String tableName = args[0];
		Path inputDir = new Path(args[1]);
		Job job = new Job(conf, NAME + "_" + tableName);
		job.setJarByClass(ImportMapper.class);
		FileInputFormat.setInputPaths(job, inputDir);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ImportMapper.class);

		// No reducers. Just write straight to table. Call
		// initTableReducerJob
		// to set up the TableOutputFormat.
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
		job.setNumReduceTasks(0);

		TableMapReduceUtil.addDependencyJars(job);
		//TableMapReduceUtil.addDependencyJars(job.getConfiguration(), com.google.common.base.Function.class);
		return job;

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = createSubmittableJob(conf, args);
		job.waitForCompletion(true);
		Date date = new Date();
		System.out.println("current time: " + date.toLocaleString());
	}
}