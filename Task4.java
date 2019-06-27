import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

    public static class UserMapper extends Mapper<Object, Text, Text, MapWritable> {          
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] values = value.toString().split(",", -1);
            for(int i = 1; i < values.length; i++){
                if(!values[i].equals("")){
                    Text user = new Text(); 
                    user.set(Integer.toString(i));
                    
                    Text movieName = new Text();
                    movieName.set(values[0]);
                    
                    IntWritable rating = new IntWritable();
                    rating.set(Integer.parseInt(values[i]));
                    
                    MapWritable movieRating = new MapWritable();
                    movieRating.put(movieName, rating);
                    
                    context.write(user, movieRating);
                }
            } 
        }
    }
    
    public static class UserReducer extends Reducer<Text, MapWritable, Text, IntWritable>{
        
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
            ArrayList<MapWritable> valueArr = new ArrayList<MapWritable>();
            
            // fuck stupid-ass hadoop that I have to recreate objects
            for(MapWritable map: values){
                Map.Entry<Writable,Writable> movieRating = map.entrySet().iterator().next();
                Text name = (Text)movieRating.getKey();
                IntWritable rating = (IntWritable)movieRating.getValue();
                
                MapWritable tmp = new MapWritable();
                tmp.put(name, rating);
                valueArr.add(tmp);
            }
            
            for(int i = 0; i < valueArr.size(); i++){
                for(int j = i + 1; j < valueArr.size(); j++){
                    MapWritable map1 = valueArr.get(i);
                    Map.Entry<Writable,Writable> movie1 = map1.entrySet().iterator().next();
                    Text movieName1 = (Text)movie1.getKey();
                    IntWritable movieRating1 = (IntWritable)movie1.getValue();
                    
                    MapWritable map2 = valueArr.get(j);
                    Map.Entry<Writable,Writable> movie2 = map2.entrySet().iterator().next();
                    Text movieName2 = (Text)movie2.getKey();
                    IntWritable movieRating2 = (IntWritable)movie2.getValue();
                    
                    if(movieRating1.equals(movieRating2)){
                        Text moviePair = new Text();
                        IntWritable similarityCount = new IntWritable();
                        similarityCount.set(1);
                        if(movieName1.toString().compareTo(movieName2.toString()) > 0){
                            moviePair.set(movieName1.toString() + "," + movieName2.toString());
                        }else{
                            moviePair.set(movieName2.toString() + "," + movieName1.toString());
                        }
                        context.write(moviePair, similarityCount);                
                    }
                }
            }
        }
    }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    // add code here
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);
    job.setMapperClass(UserMapper.class);  
    job.setReducerClass(UserReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
