import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import java.lang.Math;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class TFIDF_Cato extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TFIDF_Cato.class);
    private static final String WC_Output = "zWordCount";
    private static final String TF_Output = "zTermFrequency";
	private static final String TFIDF_Output = "zTFIDF";

    public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());

		Path InputFilepath = new Path(args[0]);

		Path WCOutputPath = new Path(WC_Output);
		if (fs.exists(WCOutputPath)) {
			fs.delete(WCOutputPath, true);
        }
        
        Path TFOutputPath = new Path(TF_Output);
		if (fs.exists(TFOutputPath)) {
			fs.delete(TFOutputPath, true);
		}

		Path TFIDFOutputPath = new Path(TFIDF_Output);
		if (fs.exists(TFIDFOutputPath)) {
			fs.delete(TFIDFOutputPath, true);
		}

		FileStatus[] FilesList = fs.listStatus(InputFilepath);
		final int totalinputfiles = FilesList.length;

        //JOB WORDCOUNT
        Job job1 = new Job(getConf(), "WordCount");
        
        job1.setJarByClass(this.getClass());
		job1.setMapperClass(WC_Map.class);
		job1.setReducerClass(WC_Reduce.class);

        job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job1, InputFilepath);
		FileOutputFormat.setOutputPath(job1, WCOutputPath);

        job1.waitForCompletion(true);
        
        //JOB TERM FREQUENCY
        Job job2 = new Job(getConf(), "TermFrequency");
        
        job2.setJarByClass(this.getClass());
		job2.setMapperClass(TF_Map.class);
		job2.setReducerClass(TF_Reduce.class);

        job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job2, InputFilepath);
		FileOutputFormat.setOutputPath(job2, TFOutputPath);

		job2.waitForCompletion(true);

		//JOB TFIDF
		Job job3 = new Job(getConf(), "TFIDF");

		job3.getConfiguration().setInt("totalinputfiles", totalinputfiles);
		
		job3.setJarByClass(this.getClass());
		job3.setMapperClass(TFIDF_Map.class);
        job3.setReducerClass(TFIDF_Reduce.class);
        
		job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job3, TFOutputPath);
		FileOutputFormat.setOutputPath(job3, TFIDFOutputPath);

        return job3.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF_Cato(), args);
		System.exit(res);
	}

	//WORD COUNT
	public static class WC_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern.compile("[^A-Za-z]+");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString().toLowerCase();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			Text currentWord = new Text();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				
				currentWord = new Text(word + "\t" + fileName + "\t");
				context.write(currentWord, one);
			}
		}
	}

	public static class WC_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
            }
            
			context.write(word, new IntWritable(sum));
		}
    }

	//TERM FREQUENCY	
	public static class TF_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern.compile("[^A-Za-z]+");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString().toLowerCase();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			Text currentWord = new Text();
			
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				
				currentWord = new Text(word + "\t" + fileName + "\t");
				context.write(currentWord, one);
			}
		}
	}

	public static class TF_Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		//@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
            }
            
            // Calculate Term Frequency in logarithmic form
			double tf = Math.log10(10) + Math.log10(sum);
            
            context.write(word, new DoubleWritable(tf));
		}
	}
	
	//TF-IDF
	public static class TFIDF_Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word_key = new Text();
		private Text filename_tf = new Text();

		public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
			String[] word_file_tf = value.toString().split("\t\t");
			String[] word_file = word_file_tf[0].toString().split("\t");
			
			this.word_key.set(word_file[0]);
			this.filename_tf.set(word_file[1] + "=" + word_file_tf[1]);
			context.write(word_key, filename_tf);
		}
	}

	public static class TFIDF_Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		private Text word_file_key = new Text();
		private double tfidf;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double docswithword = 0;
			Map<String, Double> tempvalues = new HashMap<String, Double>();
            
            for (Text v : values) {
				String[] filecounter = v.toString().split("=");
				docswithword++;
				tempvalues.put(filecounter[0], Double.valueOf(filecounter[1]));
			}

			int numoffiles = context.getConfiguration().getInt("totalinputfiles", 0);
            
			double idf = Math.log10(numoffiles / docswithword);

			for (String temp_tfidf_file : tempvalues.keySet()) {
				this.word_file_key.set(key.toString() + "\t" + numoffiles + "\t" + docswithword + "\t" + temp_tfidf_file + "\t" + idf + "\t");
                
				this.tfidf = tempvalues.get(temp_tfidf_file) * idf;
				
				context.write(this.word_file_key, new DoubleWritable(this.tfidf));
			}
		}
	}
}
