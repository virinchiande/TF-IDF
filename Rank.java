

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class Rank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Rank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   String query = "";
	  for(int i =2 ; i<args.length ; i++){  // fetching query from the arguments
		if(i==2){
			query = args[2];		
		}else{
			query = query+" "+args[i];
		}	
	   }

      Configuration conf=getConf();
      Job job =Job.getInstance(conf," DocWordCount ");
      job.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job,args[0]); // fetching input from the command line arguments
      FileOutputFormat.setOutputPath(job,new Path("/home/cloudera/Desktop/Termfreqoutput")); //path to store term frequency output
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setMapOutputKeyClass( Text .class);
      job.setMapOutputValueClass( IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      job.waitForCompletion( true);

      Configuration confg=getConf();
      FileSystem fs= FileSystem.get(confg);
      final int NoOfFiles = fs.listStatus(new Path(args[0])).length; /* calculating the number of files in the input folder */
      confg.set("NumberOfFiles",String.valueOf(NoOfFiles));
      confg.set("Query", query.toLowerCase());
      Job job2 = Job.getInstance(confg," TFIDF ");
      job2.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPath(job2,new Path("/home/cloudera/Desktop/Termfreqoutput"));  /* taking output from job1 and providing it as input to job2 */
      FileOutputFormat.setOutputPath(job2,  new Path("/home/cloudera/Desktop/TfidfOutput")); // path to store TF-IDF output
      job2.setMapperClass( Map2 .class);
      job2.setReducerClass( Reduce2 .class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);
      job2.setOutputKeyClass( Text.class);
      job2.setOutputValueClass( DoubleWritable.class);
      
     
      if(query == "")  // if the query does not contain any words then return TFIDF job status
    	  return job2.waitForCompletion( true)? 0 : 1;
    	else{			
    		 job2.waitForCompletion(true)  ;
    	     Job job3  = Job .getInstance(getConf(), " Search ");
    	     job3.setJarByClass( this .getClass());

    	     FileInputFormat.addInputPath(job3,  new Path("/home/cloudera/Desktop/TfidfOutput")); /* taking output from job2 and providing it as input to job3 */
    	     FileOutputFormat.setOutputPath(job3,  new Path("/home/cloudera/Desktop/SearchOutput"));  /* path to store search output */
    	    
    	     job3.setMapperClass(Map3 .class);
    	     job3.setReducerClass( Reduce3 .class);
    	     job3.setOutputKeyClass( Text .class);
    	     job3.setOutputValueClass( DoubleWritable .class);
    	    
    	     job3.waitForCompletion( true);
    	     Job job4  = Job .getInstance(getConf(), " Rank ");
    	     job4.setJarByClass( this .getClass());

    	     FileInputFormat.addInputPath(job4, new Path("/home/cloudera/Desktop/SearchOutput"));  
    	     FileOutputFormat.setOutputPath(job4,new Path(args[1]));  /*final rank result are stored in output path*/    
    	     job4.setMapperClass( MAP4 .class);
    	     job4.setReducerClass( Reduce4 .class);   /*Rank Job*/
    	     job4.setOutputKeyClass( Text .class);
    	     job4.setOutputValueClass( Text .class);
    	    	
    	     return job4.waitForCompletion( true)  ? 0 : 1;	
   }
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
     // private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         FileSplit fileSplit = (FileSplit)context.getInputSplit();
         String filename = fileSplit.getPath().getName(); // fetching file name
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY.split(line)) {
            if (word.trim().replaceAll(" ", "").replaceAll("\t", "").isEmpty()) {
               continue;
          }
           
            
           // converting the word to lowercase and adding filename in the required format
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
          termfrequency=1+Math.log10(sum); // calculating term frequency of a particular word
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
         
         for(String arrr: SPACE.split(line)) // splitting key value pairs using Space as a delimiter
         {
        	 if (arrr.isEmpty()) {
                 continue;
            }
        	 firstarray[c]=arrr;
        	 c++;
         }
         c=0;
         String oldkey=firstarray[0]; // storing word and filename with Hashes in oldkey
        
         String oldvalue=firstarray[1]; // storing term frequency of that word in oldvalue
         String[] secondarray=new String[100];
         for(String prrr: HASHES.split(oldkey)){ // splitting word and filename using hashes
        	 
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
         String s[]= new String[n];
         double idf=0.0;
         for ( Text  count: counts) {
        	 
        	String old= count.toString();
        	 s[c]=old;  // storing the Iterable values in an array
            c+=1;
         }
         idf=Math.log10(1+(n/c));    // calculating IDF value
          String oldkey=word.toString();
          for(int l=0;l<c;l++){    // storing filename and TF value in an array
        	  String[] a=EQUALSTO.split(s[l]);
        	  String[] array=new String[2];
              array[0] = a[0];
     		 array[1] = a[1];
        	 
        	  String newkey=oldkey+"#####"+array[0];  // word and filename
        	  Double value=Double.parseDouble(array[1]);
        	  Double newval=value*idf;		// calculating TF-IDF value
        	  Text finalkey=new Text(newkey);
              context.write(finalkey,new DoubleWritable(newval));
            
        	  
          }
      }
	@Override
	public void configure(JobConf job) {
		
		n=Integer.parseInt(job.get("NumberOfFiles")); //   Number of files in the input path
		
	}
   }
 
 public static class Map3 extends Mapper<LongWritable ,Text ,Text ,DoubleWritable > {
 	
   	 private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
   	private static final Pattern HASHES= Pattern.compile("#####");
	 private String FinalKey,FinalVaule;

    	 public void map( LongWritable offset,  Text lineText,  Context context)
       	throws  IOException,  InterruptedException {
    		 Configuration confg = context.getConfiguration();
             String query = confg.get("Query");// Search query is being fetched from confg
             String line  = lineText.toString();
             String[] lines = line.split(System.getProperty("line.separator"));

		       for ( String string : lines) {
			for ( String word  : WORD_BOUNDARY .split(query)) {// splitting the query using space as a delimiter
 		          if (word.isEmpty()) {
  		            continue;
   		        }
 		         FinalKey = HASHES.split(string)[0];
       		    if(FinalKey.equals(word)){              //comparing the words
    	    	   	String currentkey = HASHES.split(string)[1].split("\\s")[0];  // storing file name if words are same
    	    	   	FinalVaule = HASHES.split(string)[1].split("\\s")[1];// storing Tfidf value
       		    	double currentvaule  = Double.valueOf(FinalVaule);
     			context.write(new Text(currentkey),new DoubleWritable(currentvaule));
       		
       			    }
		           
				}
		           
		       }
    	 }
	}
 public static class Reduce3 extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
     @Override 
     public void reduce( Text file,  Iterable<DoubleWritable > TfidfList,  Context context)
        throws IOException,  InterruptedException {
        double Tfidf  = 0;
        for ( DoubleWritable count  : TfidfList) {
           Tfidf  += count.get();/* Sum of Tfidf values of all the words in search query in a particular file */
        }
         
        context.write(file,  new DoubleWritable(Tfidf));
     }
  }
 
 public static class MAP4 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
     
     public void map( LongWritable offset,  Text lineText,  Context context)
       throws  IOException,  InterruptedException {

        String line  = lineText.toString();
        Text currentkey  = new Text("Vijju");		
        String[] lines = line.split(System.getProperty("line.separator"));

        for ( String Line : lines) {   
        	//line = line.trim().replaceAll("\\s", "=");
           context.write(currentkey,new Text(Line.trim().replaceAll("\\s", "=")));/*output from search is taken and changed so as to process in reducer*/
        }
     }
  }
 public static class Reduce4 extends Reducer<Text ,Text ,  Text ,DoubleWritable > {
     @Override 
     public void reduce( Text word,  Iterable<Text > list,  Context context)
        throws IOException,  InterruptedException {
        TreeMap<Double, String> TM = new TreeMap<Double,String>(Collections.reverseOrder());
         for (Text sort : list) {   
            
                      String file = sort.toString().split("=")[0];
                      double number;
                    		  if(TM.containsKey(Double.valueOf(sort.toString().split("=")[1]))){   /* Checking if the same ranked file exists or not*/
                    				  number = Double.valueOf(sort.toString().split("=")[1])-0.0000005;
                    		  }
                    		  else
                    			  number = Double.valueOf(sort.toString().split("=")[1]); 
                      TM.put(number, file);/*entering input into TreeMap, which automatically sort the input*/
                   }
         Set<Entry<Double, String>> set = TM.entrySet();
           
           Iterator<Entry<Double, String>> i = set.iterator();
           
           while(i.hasNext()) { 
              Entry<Double, String> node = (Entry<Double, String>)i.next();
              context.write(new Text((String) node.getValue()),  new DoubleWritable((Double) node.getKey()));/*print the key and values of tree, according to the rank*/
           }
     }
  }
 
}


