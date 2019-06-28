import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  public static class RatingMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable user = new IntWritable();
    private IntWritable rating = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",",-1);
      for(int i = 1; i < line.length; i++){
        if(line[i].length() > 0){
          user.set(i);
          rating.set(1);
          context.write(user, rating);
        }
      }
    }
  }

   public static class RatingReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
     public void reduce(IntWritable key, Iterable<IntWritable> ratings, Context context) throws IOException, InterruptedException {
       int count = 0;
       for(IntWritable rating : ratings) {
				  count++;
			  }
      context.write(key, new Text(String.format("%1.2f",count)));
     }
   }



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");

    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    job.setMapperClass(RatingMapper.class);
    job.setReducerClass(RatingReducer.class);
    job.setOutputKeyClass(IntWritable.class);
	  job.setOutputValueClass(IntWritable.class);
    // add code here

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}