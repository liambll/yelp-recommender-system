package assignment2a;
// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 
// package ca.sfu.whatever;
 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Random;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class EulerEstimator extends Configured implements Tool {
 
    public static class TokenizerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Random random = new Random();
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	//get filename's hashcode
        	long fileNameHash = ((FileSplit)context.getInputSplit()).getPath().getName().hashCode();
        	
        	// combine hashcode with line offset to get a seed with high probability of uniqueness
        	long seed = Long.parseLong(""+ fileNameHash + key.get());
        	
        	random.setSeed(seed);
        	long iterations = Integer.parseInt(value.toString());
        	long count = 0;
        	for (long i=1; i <= iterations; i++) {
        		double sum = 0;
        		while (sum < 1) {
        			sum += random.nextDouble();
        			count++;
        		}
        	}
        	
        	context.getCounter("Euler", "iterations").increment(iterations);
        	context.getCounter("Euler", "count").increment(count);
        }
    }
 
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "euler estimator");
        job.setJarByClass(EulerEstimator.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(LongSumReducer.class);
        //job.setReducerClass(LongSumReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        //TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}