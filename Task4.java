import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
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

public class Task4 {

    public static class MapSideJoinMapper extends Mapper<Object, Text, Text, IntWritable> {
        private HashMap<String, List<Integer>> movieRatingMap = new HashMap<String, List<Integer>>();
        private BufferedReader bufferedReader;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String file = fileSplit.getPath().getName();
            Path[] cachedFiles = context.getLocalCacheFiles();
            String currentLine = "";
            
            for (Path p : cachedFiles) {
                String currentFileName = p.getName().toString().trim();
                if (currentFileName.equals(file)) {
                    try {
                        bufferedReader = new BufferedReader(new FileReader(p.toString()));
                        while ((currentLine = bufferedReader.readLine()) != null) {
                            String[] tokens = currentLine.toString().split(",", -1);
                            Integer dummyPlaceholder = new Integer(-1000);
                            List<Integer> tempArr = new ArrayList<Integer>();
                            tempArr.add(dummyPlaceholder);
                           
                            for(int i = 1; i < tokens.length; i++){
                                Integer rate;
                                if(!tokens[i].equals("")){
                                    rate = Integer.parseInt(tokens[i]);    
                                }else{
                                    rate = dummyPlaceholder;
                                }
                                
                                tempArr.add(rate);
                            }
                            movieRatingMap.put(tokens[0], tempArr);
                        }
                    } catch (Exception e) {
                        System.out.println("Error reading files from cache. Exiting.");
                        e.printStackTrace();
                    } finally {
                        if (bufferedReader != null) {
                            bufferedReader.close();
                        }
                    }
                }
            }
            
            super.setup(context);
        }
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] values = value.toString().split(",", -1);
            String currentMovieName = values[0];

            for(Map.Entry<String, List<Integer>> entry : movieRatingMap.entrySet()){
                String movieName = entry.getKey();
                List<Integer> ratings = entry.getValue();
                if(!movieName.equals(currentMovieName)){
                    
                    //format the movie name as the manual requests
                    String movieNamePair;
                    if(movieName.toString().compareTo(currentMovieName.toString()) < 0){
                        movieNamePair = movieName.toString() + "," + currentMovieName.toString();
                    }else{
                        movieNamePair = currentMovieName.toString() + "," + movieName.toString();
                    }
                    
                    int similarityCount = 0;
                    //both array index start at 1 cause 0 is movie name
                    for(int i = 1; i < values.length; i++){ 
                        if(!values[i].equals("")){
                            int currentMovieScore = Integer.parseInt(values[i]);
                            int cachedMovieScore = ratings.get(i).intValue();
                            if((currentMovieScore > 0) && 
                               (cachedMovieScore > 0) && 
                               (currentMovieScore == cachedMovieScore)){
                                similarityCount += 1;
                            }
                        }
                        
                    }
                    
                    Text outKey = new Text();
                    outKey.set(movieNamePair);
                    IntWritable outVal = new IntWritable();
                    outVal.set(similarityCount);
                    context.write(outKey, outVal);
                }
            }
        }
    }   
    
    public static class MyReducer extends Reducer<Text, IntWritable, Text, Text>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            Text outVal = new Text();
            int sum = 0;
            for (IntWritable similarityCount: values){
                sum += similarityCount.get();
            }        
            outVal.set(Integer.toString(sum/2));
            context.write(key, outVal);    
        }
    }


    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // add code here
    Job job = Job.getInstance(conf, "Task4");  
    job.setJarByClass(Task4.class);
    job.setMapperClass(MapSideJoinMapper.class); 
    job.setReducerClass(MyReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.addCacheFile(new URI(otherArgs[0]));
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
