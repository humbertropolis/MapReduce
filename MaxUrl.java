  import java.io.IOException;
  import java.util.StringTokenizer;

  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.io.IntWritable;
  import org.apache.hadoop.io.Text;
  import org.apache.hadoop.mapreduce.Job;
  import org.apache.hadoop.mapreduce.Mapper;
  import org.apache.hadoop.mapreduce.Reducer;
  import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
  import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class MaxUrl {

public static class TokenizerMapper
         extends Mapper<Object, Text, Text, IntWritable>{

      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
      private String keyServer = "edams.ksc.nasa.gov";
      private String server = "";

      public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(),"\t");

        while (itr.hasMoreTokens()) {

          word.set(itr.nextToken());
          server=word.toString();

          if(server.equals(keyServer)){
            for(int i=0;i<=3;i++){
              word.set(itr.nextToken());
            }
            context.write(word, one);
          }
        }
      }
    }

 

    public static class IntSumReducer
         extends Reducer<Text,IntWritable,Text,IntWritable> {
      int max =0;
      Text maxWord = new Text();

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int sum=0;
          for (IntWritable value : values) 
            {
                sum += value.get();
            }

            if(sum > max)
            {
                max = sum;
                maxWord.set(key);
            }

      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxWord, new IntWritable(max));
      }
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "max url");
      job.setJarByClass(MaxUrl.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }