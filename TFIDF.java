
import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.Math;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new WordCount(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {

      Configuration conf=getConf();
      Job job =Job.getInstance(conf," TermFrequency ");
      job.setJarByClass( this .getClass());
      FileInputFormat.addInputPath(job, args[0]);
      FileOutputFormat.setOutputPath(job,new Path("/home/cloudera/Desktop/Output1")); /* Intermediate path for outputof TermFrequency */
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setMapOutputKeyClass( Text .class);
      job.setMapOutputValueClass( IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      job.waitForCompletion( true);

      Configuration confg=getConf();
      FileSystem fs= FileSystem.get(confg);
      final int NoOfFiles = fs.listStatus(args[0]).length; /* calculating number of files in the input*/
      confg.set("NumberOfFiles",String.valueOf(NoOfFiles));
      Job job2 = Job.getInstance(confg," TFIDF ");
      job2.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPath(job2,new Path("/home/cloudera/Desktop/Output1")); /* Giving termfrequency output as input to TFIDF*/
      FileOutputFormat.setOutputPath(job2,  new Path(args[1]));
      job2.setMapperClass( Map2 .class);
      job2.setReducerClass( Reduce2 .class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);
      job2.setOutputKeyClass( Text.class);
      job2.setOutputValueClass( DoubleWritable.class);
      return job2.waitForCompletion( true)? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         FileSplit fileSplit = (FileSplit)context.getInputSplit();
         String filename = fileSplit.getPath().getName(); /* Getting the file name */
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY.split(line)) {
            if (word.trim().replaceAll(" ", "").replaceAll("\t", "").isEmpty()) {
               continue;
          }
           
            
           /* Spaces are removed and converting to lowercase and then writing it in the required format*/
            currentWord.set(word.trim().replaceAll(" ", "").replaceAll("\t", "").toLowerCase()+"#####"+filename);
          
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         double termfrequency=0.0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
          termfrequency=1+Math.log10(sum); /* calculating the TermFrequency */
         context.write(word, new DoubleWritable(termfrequency));
      }
   }
   public static class Map2 extends Mapper<LongWritable , Text,  Text ,  Text > {
      
       private static final Pattern SPACE = Pattern.compile("\\s+");
       private static final Pattern HASHES= Pattern.compile("#####");
        public void map(LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         
         
         Text currentKey  = new Text();
         Text currentValue= new Text();
         String line = lineText.toString();
         String[] firstarray=new String[100];
         int c=0;
         
         for(String arrr: SPACE.split(line))  //  using space as a delimiter splitting the key value pairs
         {
        	 if (arrr.isEmpty()) {
                 continue;
            }
        	 firstarray[c]=arrr;
        	 c++;
         }
         c=0;
         String oldkey=firstarray[0]; // storing word and the filename with hashes in the oldkey
        
         String oldvalue=firstarray[1];  // storing term frequency of that word in oldvalue
         String[] secondarray=new String[100];
         for(String prrr: HASHES.split(oldkey)){   // splitting word and filename using hashes
        	
        	 secondarray[c]=prrr;
        	 c++;
         }
        
         String newKey=secondarray[0];

         String newValue=secondarray[1]+"="+oldvalue; // filename and TF value
         currentKey.set(newKey);
         currentValue.set(newValue);

         context.write(currentKey,currentValue);
         }}

 public static class Reduce2 extends Reducer<Text , Text ,  Text ,  DoubleWritable > implements JobConfigurable{
      public static int n;
      private static final Pattern EQUALSTO = Pattern.compile("=");
      @Override
      public void reduce( Text word,  Iterable<Text> counts,  Context context)
         throws IOException,  InterruptedException {
         int c=0;
         String s[]= new String[1000];
         double idf=0.0;
         for ( Text  count: counts) {
        	 
        	String old= count.toString();
        	 s[c]=old; // storing the Iterable values in an array
            c+=1;
         }
         idf=Math.log10(1+(n/c));  // calculating IDF value
          String oldkey=word.toString();
          for(int l=0;l<c;l++){  // storing filename and TF value in an array
        	  String[] a=EQUALSTO.split(s[l]);
        	  String[] array=new String[10];
              array[0] = a[0];
     		 array[1] = a[1];
        	 
        	  String newkey=oldkey+"#####"+array[0];  // word and filename
        	  Double value=Double.parseDouble(array[1]);
        	  Double newval=value*idf;  // calculating TF-IDF value
        	  Text finalkey=new Text(newkey);
              context.write(finalkey,new DoubleWritable(newval));
            
        	  
          }
      }
	@Override
	public void configure(JobConf job) {
		
		n=Integer.parseInt(job.get("NumberOfFiles")); //   Number of files in the input path
		
	}
   }
}

