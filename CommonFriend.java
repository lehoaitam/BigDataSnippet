package mypackage;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CommonFriend extends Configured implements Tool {
  
  static Log logMap = LogFactory.getLog(CFMapper.class);
  static Log logReduce = LogFactory.getLog(CFReducer.class);
  public static class CFMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {
    
    private Text keyword = new Text();
    private Text friends = new Text();
    public static int count = 0;
    
    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, Text> output, 
                    Reporter reporter) throws IOException {
      String line = value.toString();
      //out log
	  logMap.info("map input:" + count++ + ":" + line);
      String []meAndFriends = line.split(">");
      String me = meAndFriends[0].trim();      
      friends.set(meAndFriends[1]);
      StringTokenizer itr = new StringTokenizer(meAndFriends[1]);
      while (itr.hasMoreTokens()) {
    	String friend = itr.nextToken().trim();
    	if(me.compareTo(friend) < 0)
    		keyword.set(me + "," +  friend);
    	else
    		keyword.set(friend + "," +  me);
        output.collect(keyword, friends);
        logMap.info("map output:" + "key=" + keyword + ",value=" + friends);
      }
    }
  }
  
  public static class CFReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    public static int count = 0;
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, 
                       Reporter reporter) throws IOException {
     
      ArrayList<String[]> arr = new ArrayList<String[]>();
      logReduce.info("reduce input:" + count++ + ":key=" + key);
      int countTemp = 0;
      while (values.hasNext()) {
    	  String temp = values.next().toString();
    	  //out log
    	  System.out.println(temp);
    	  logReduce.info("item:" + countTemp++ + ":" + temp);
    	  String[] listFriend = temp.split(" ");
    	  arr.add(listFriend);
      }
      String commonFriendStr = " > ";
      //find common friend 
      HashSet<String> commonElements = new HashSet<String>();
      for(String[] friends: arr){
    	  Arrays.sort(friends);
    	  for(String friend: friends){
    		  if(commonElements.contains(friend))
    			  commonFriendStr += friend + " ";
    		  else
    			  commonElements.add(friend);
    	  }
      }
      //
      Text commonFriendText = new Text();
      
      commonFriendText.set(commonFriendStr);
      output.collect(key, commonFriendText);
    }
  }
  
  static int printUsage() {
    System.out.println("CommonFriend [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
 
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), CommonFriend.class);
    conf.setJobName("CommonFriend");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(Text.class);
    
    conf.setMapperClass(CFMapper.class); 
    conf.setReducerClass(CFReducer.class);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        
    JobClient.runJob(conf);
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
	System.out.println("Common friend searching ....");
    int res = ToolRunner.run(new Configuration(), new CommonFriend(), args);
    
    System.exit(res);
  }

}
