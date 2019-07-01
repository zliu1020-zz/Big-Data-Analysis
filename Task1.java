import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.ArrayList;

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

public class Task1 {

  // add code here
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    Text movies = new Text();
    Text users = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] values = value.toString().split(",", -1);
        
        String movieName = values[0];
        movies.set(movieName);
        
        int max = 0; 
        ArrayList<Integer> result = new ArrayList<Integer>();
        for(int i = 1; i < values.length; i++){
    	    String val = values[i];
            int rating = val.equals("") ? -1 : Integer.parseInt(val);
            if(rating == max){
                result.add(i);
            }else if(rating > max){
                max = rating;
                result.clear();
                result.add(i);
            }
        }
        //sort user ids in numerical asending order
        Collections.sort(result);
        StringBuilder resultStr = new StringBuilder();
        for(Integer user: result){
            resultStr = resultStr.length() > 0 ? resultStr.append(",").append(user) : resultStr.append(user);
        }
        users.set(resultStr.toString());
        context.write(movies, users);
    }
  } 
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);  
    // add code here

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
