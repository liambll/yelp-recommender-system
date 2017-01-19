package assignment1b;
// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 
// package ca.sfu.whatever;
 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import assignment1b.LongPairWritable;
import assignment1b.MultiLineJSONInputFormat;
 
public class RedditAverage extends Configured implements Tool {
 
    public static class SubRedditMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
        private LongPairWritable count_total = new LongPairWritable();
        private Text subreddit = new Text();
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	ObjectMapper json_mapper = new ObjectMapper();
        	JsonNode data = json_mapper.readValue(value.toString(), JsonNode.class);
            String subredditString = data.get("subreddit").textValue();
            long score = data.get("score").longValue();
            if (subredditString.length() > 0) {
            	subreddit.set(subredditString);
    			count_total.set(1, score);
    			context.write(subreddit, count_total);
            }
        }
    }
    
    public static class SubRedditCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
    	private LongPairWritable count_total = new LongPairWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long count = 0;
            long total = 0;
            for (LongPairWritable val : values) {
            	count += val.get_0();
            	total += val.get_1();
            }
            count_total.set(count, total);
            context.write(key, count_total);
        }
    }
 
    public static class SubRedditReducer
    extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long count = 0;
            long total = 0;
            for (LongPairWritable val : values) {
            	count += val.get_0();
            	total += val.get_1();
            }
            double avg=0;
            if (count > 0) {
            	avg = total/count;
            }
            result.set(avg);
            context.write(key, result);
        }
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "reddit average");
        job.setJarByClass(RedditAverage.class);
 
        job.setInputFormatClass(MultiLineJSONInputFormat.class);
 
        job.setMapperClass(SubRedditMapper.class);
        job.setCombinerClass(SubRedditCombiner.class);
        job.setReducerClass(SubRedditReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}