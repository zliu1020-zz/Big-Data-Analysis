import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

    public static class MapSideJoinMapper extends Mapper<Object, Text, Text, IntWritable> {
        private HashMap<String, List<Integer>> movieRatingMap = new HashMap<String, List<Integer>>();
        private BufferedReader brReader;
        private static HashMap<String, Boolean> visited = new HashMap<String, Boolean>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String strLineRead = "";
            for (Path eachPath : cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("in3.txt")) {
                    try {
                        brReader = new BufferedReader(new FileReader(eachPath.toString()));
                        while ((strLineRead = brReader.readLine()) != null) {
                            String[] tokens = strLineRead.toString().split(",", -1);
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
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }finally {
                        if (brReader != null) {
                            brReader.close();
                        }
                    }
                }
            }
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
                        movieNamePair = movieName.toString() + "_" + currentMovieName.toString();
                    }else{
                        movieNamePair = currentMovieName.toString() + "_" + movieName.toString();
                    }
                    
                    //skip the movie if the pair has been compared already
                    if(visited.containsKey(movieNamePair) && visited.get(movieNamePair).equals(Boolean.TRUE)){
                        continue;
                    }else if(!visited.containsKey(movieNamePair)){
                        visited.put(movieNamePair, Boolean.TRUE);
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
            for (IntWritable similarityCount: values){
                Text outVal = new Text();
                outVal.set(Integer.toString(similarityCount.get()));
                context.write(key, outVal);
            }            
        }
    }

    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    DistributedCache.addCacheFile(new URI("/a2_inputs/in3.txt"), conf);
          
    // add code here
    Job job = Job.getInstance(conf, "Task4");  
    job.setJarByClass(Task4.class);
    job.setMapperClass(MapSideJoinMapper.class); 
    job.setReducerClass(MyReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
