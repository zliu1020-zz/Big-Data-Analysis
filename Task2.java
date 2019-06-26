import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {

    // add code here
    public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, IntWritable> {
        NullWritable dummyKey = NullWritable.get(); 
        IntWritable result = new IntWritable();
          
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] values = value.toString().split(",", -1);
            int counter = 0;
            for(int i = 1; i < values.length; i++){
                if(!values[i].equals("")){
                    counter += 1;
                }
            }
            result.set(counter);
            context.write(dummyKey, result);
        }
    }
    
    
    public static class IntSumReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable>{
        private IntWritable result = new IntWritable();
        public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    // add code here
    Job job = Job.getInstance(conf, "Task2");
    job.setJarByClass(Task2.class);
    job.setMapperClass(TokenizerMapper.class);  
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
