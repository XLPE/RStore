
/**
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
package DataCubeRefresh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HStreamingJobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.HStreamingInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextHStreamingInputFormat;
import org.apache.hadoop.mapreduce.lib.output.HStreamingOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextHStreamingOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts matching regexs from input streams and counts them.
 */
public class Grep extends Configured implements Tool {

  /** 
   * A text-based mapper that swaps keys and values. 
   */
  public static class InverseTextMapper<K, V>  
    extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text > {
    
    /** 
     * The inverse function on text input.  Input keys and values are swapped.
     * @param key key
     * @param value value
     * @param context map context
     * @throws IOException if an input or output exception occurred
     * @throws InterruptedException if an interrupted exception occurred
     */
    @Override
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      if (value == null)
        return;
      
      String patternCount[] = value.toString().split("\t");

      if (patternCount.length != 2) {
        System.out.println("Invalid pattern/count value :"+value);
        return;
      }
      context.write(new LongWritable(Integer.parseInt(patternCount[1])), new Text(patternCount[0]));
    }
   
  }

  /** 
   * A regex mapper.
   */
  public static class RegexMapper<K> extends org.apache.hadoop.mapreduce.Mapper<K, Text, Text, LongWritable> {

    static final String REGEX_OPT = "mapred.mapper.regex";
    static final String GROUP_OPT = "mapred.mapper.regex.group";

    static private Pattern pattern = null;
    static private int group;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

      // construct pattern and group as singletons to keep the overhead over multiple instantiations down
      if (pattern == null) {
        pattern = Pattern.compile(context.getConfiguration().get(REGEX_OPT, ".*"));
        group = context.getConfiguration().getInt(GROUP_OPT, 0);
      }
    }

    @Override
    protected void map(K key, Text value, Context context) throws IOException, InterruptedException {
      String text = value.toString();
      Matcher matcher = pattern.matcher(text);
      while (matcher.find()) {
        context.write(new Text(matcher.group(group)), new LongWritable(1));
      }
    }
  }

  /** Grep singleton. */
  private Grep() {}

  /**
   * Run function.
   * @param args arguments
   * @return error code
   * @throws Exception if an exception occurs
   */
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Grep <inUrl> <outUrl> <regex> [<group>]");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Job grepJob = new Job(getConf());
    Job sortJob = new Job(getConf());

    String tempStreamTag = UUID.randomUUID().toString();

    try {
      grepJob.setJobName("grep-search");

      TextHStreamingInputFormat.addInputStream(grepJob, 1000, 600, -1, "", false, args[0]);
      HStreamingJobConf.setIsStreamingJob(grepJob, true);
      grepJob.setMapperClass(RegexMapper.class);
      grepJob.getConfiguration().set("mapred.mapper.regex", args[2]);
      if (args.length == 4)
        grepJob.getConfiguration().set("mapred.mapper.regex.group", args[3]);

      grepJob.setCombinerClass(LongSumReducer.class);
      grepJob.setReducerClass(LongSumReducer.class);
      grepJob.setInputFormatClass(TextHStreamingInputFormat.class);
      grepJob.setOutputFormatClass(TextHStreamingOutputFormat.class);
      HStreamingOutputFormat.setOutputStreamTag(grepJob, tempStreamTag);
      grepJob.setOutputKeyClass(Text.class);
      grepJob.setOutputValueClass(LongWritable.class);
      grepJob.setJobName("grep-search");
      grepJob.setJarByClass(this.getClass());

      grepJob.submit();

      sortJob.setJobName("grep-sort");
      sortJob.setInputFormatClass(TextHStreamingInputFormat.class);
      HStreamingJobConf.setIsStreamingJob(sortJob, true);

      // add previous stream partition/reducer 0 as input. 
      HStreamingInputFormat.addInputStreamTag(sortJob, tempStreamTag, 0);

      sortJob.setMapperClass(InverseTextMapper.class);
      sortJob.setNumReduceTasks(1);                 // single output stream
      sortJob.setOutputFormatClass(TextHStreamingOutputFormat.class);
      TextHStreamingOutputFormat.setOutputPath(sortJob, args[1]);
      sortJob.setSortComparatorClass(               // sort by decreasing fre
              LongWritable.DecreasingComparator.class);
      sortJob.setJarByClass(this.getClass());
      sortJob.submit();

      return sortJob.waitForCompletion(true) ? 0 : 1;
    } catch (Exception e) {
      e.printStackTrace();
      try {
        grepJob.killJob();
      } catch (Exception e1) {
        // ignore
      }
      try {
        sortJob.killJob();
      } catch(Exception e2) {
        // ignore
      }
    }
    return 0;
  }

  /**
   * Entry point.
   * @param args command line arguments
   * @throws Exception if an exception occurs
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Grep(), args);
    System.exit(res);
  }

}