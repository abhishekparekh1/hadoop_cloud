
package org.myorg;
	
import java.io.IOException;
import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class WordCountDistributed {
	
	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	     private final static IntWritable one = new IntWritable(1);
	     private Text word = new Text();
	
	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       
			StringTokenizer wordList = new StringTokenizer(value.toString());
		String currentToken = null;
		while (wordList.hasMoreTokens()) {
			currentToken = wordList.nextToken();
			/*if the current word is present in the list of words from small
			file then only generate the <word,1> key/value pair*/
			if (searchWordList.contains(currentToken)) {
				word.set(currentToken);
				output.collect(word, one);
			}
		}
	   }
		private void parseSearchFile(String searchFile) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					searchFile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(line);
				String token = null;
				while (tokenizer.hasMoreTokens()) {
					token = tokenizer.nextToken();
					searchWordList.add(token);
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			System.err.println("Caught exception while opening cached file : "
					+ StringUtils.stringifyException(e));
		} catch (IOException e) {
			System.err.println("Caught exception while reading cached file : "
					+ StringUtils.stringifyException(e));
		}
		protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Path[] searchListFiles = new Path[2];
		try {
			searchListFiles = context.getLocalCacheFiles(); // get the path on local disk of the task nodes for the cached file
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Caught exception while getting cached file : "
					+ StringUtils.stringifyException(e));
		}
		parseSearchFile(searchListFiles[0].toString());
		super.setup(context);

	}

	}
	}
	
	   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       int sum = 0;
	       while (values.hasNext()) {
	         sum += values.next().get();
	       }
	       output.collect(key, new IntWritable(sum));
	     }
	   }
	public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName("wordcountdistributed");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat .setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);

        JobClient.runJob(conf);
        return 0;
    }
	   public static void main(String[] args) throws Exception {
	     public static void main(String[] args) throws Exception {
	     int res = ToolRunner.run(new Configuration(), new WordCount(), args); 
	     System.exit(res);
		}	
	   }
	}

