import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MaxTemp extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private IntWritable temp = new IntWritable();
		private Text year = new Text(); /* Key should be text */

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		    String line = value.toString();

		    temp.set(Integer.parseInt(line.substring(88,92)));
		    	if(line.substring(87,88).equals("-")){
		    		temp.set(Integer.parseInt(temp.toString())*-1);
		    	}
		    year.set(line.substring(15,19));
		    int quality = Integer.parseInt(line.substring(92,93));

		    if(temp.equals(new IntWritable(9999))) {
		    	temp.set(0);
		    }

		    if(quality != 0 && quality != 1 && quality!=4 && quality !=5 && quality !=9) {
		    	temp.set(0);
		    }

		    output.collect(year,temp);

		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		    int maxTemp = 0;
		    int valuesTemp = 0;

		    while (values.hasNext()) {
		    	valuesTemp = Integer.parseInt(values.next().toString());
				if (valuesTemp > maxTemp) {
					maxTemp = valuesTemp;
				}
			}
		    output.collect(key, new IntWritable(maxTemp));
		}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), MaxTemp.class);
	conf.setJobName("maxtemp");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new MaxTemp(), args);
	System.exit(res);
    }
}