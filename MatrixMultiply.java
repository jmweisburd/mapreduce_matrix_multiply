import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiply {

public static class MatrixMapper1 extends Mapper<LongWritable, Text, IntWritable, MatrixTuple>{

  private IntWritable newkey = new IntWritable();
  private MatrixTuple mt = new MatrixTuple();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] vals = value.toString().split(",");
    int mNum = Integer.parseInt(vals[0]);
    int rNum = Integer.parseInt(vals[1]);
    int cNum = Integer.parseInt(vals[2]);
    int v = Integer.parseInt(vals[3]);

    if (mNum==0) {
      mt.setAll(mNum, rNum, v);
      newkey.set(cNum);  
      context.write(newkey, mt);
    }
    else if (mNum == 1) {
      mt.setAll(mNum, cNum, v);
      newkey.set(rNum);
      context.write(newkey, mt);
    }
  }
}

public static class MatrixReducer1 extends Reducer<IntWritable,MatrixTuple,Text,Text> {
  private IntWritable result = new IntWritable();

  public void reduce(IntWritable key, Iterable<MatrixTuple> tuples, Context context) throws IOException, InterruptedException {
    List<MatrixTuple> fromZero = new ArrayList<MatrixTuple>();
    List<MatrixTuple> fromOne = new ArrayList<MatrixTuple>();

    for (MatrixTuple t : tuples) {
      if (t.matrixNum.get() == 0) {
        fromZero.add(new MatrixTuple(t));
      }
      else {
        fromOne.add(new MatrixTuple(t));
      }
    }

    for (MatrixTuple z : fromZero) {
      for (MatrixTuple o : fromOne) {
        int mval = z.value.get() * o.value.get();
        String s_mval = Integer.toString(mval);
        String index = Integer.toString(z.oppNum.get()) + "," + Integer.toString(o.oppNum.get()) + ",";
        context.write(new Text(index), new Text(s_mval));
      }
    } 
  }
}

public static class MatrixMapper2 extends Mapper<LongWritable,Text,Text,IntWritable> {
  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] vals = value.toString().split(",");
    int dp = Integer.parseInt(vals[2].replaceAll("\\s+",""));
    String ind = vals[0] + "," + vals[1];
    Text nkey = new Text(ind);
    context.write(nkey, new IntWritable(dp));
  }
}

public static class MatrixReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
  private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable v : vals) {
      sum += v.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}

/*
public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "word count");
  job.setJarByClass(MatrixMultiply.class);
  job.setMapperClass(MatrixMapper1.class);
  //job.setCombinerClass(MatrixReducer1.class);
  job.setReducerClass(MatrixReducer1.class);
  job.setMapOutputKeyClass(IntWritable.class);
  job.setMapOutputValueClass(MatrixTuple.class);
  job.setOutputKeyClass(IntWritable.class);
  job.setOutputValueClass(IndexTuple.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}*/

public static void main(String[] args) throws Exception {
  //First Job - Dot product of all indicies
  Configuration conf = new Configuration();
  Job job1 = Job.getInstance(conf, "Dot Products");


  job1.setJarByClass(MatrixMultiply.class);

  job1.setMapperClass(MatrixMapper1.class);
  job1.setReducerClass(MatrixReducer1.class);
  job1.setMapOutputKeyClass(IntWritable.class);
  job1.setMapOutputValueClass(MatrixTuple.class);
  job1.setOutputKeyClass(Text.class);
  job1.setOutputValueClass(Text.class);

  job1.setInputFormatClass(TextInputFormat.class);
  job1.setOutputFormatClass(TextOutputFormat.class);

  TextInputFormat.addInputPath(job1, new Path("input"));
  TextOutputFormat.setOutputPath(job1, new Path("temp"));

  job1.waitForCompletion(true);

  Job job2 = Job.getInstance(conf, "Sum of Dot Products");
  job2.setJarByClass(MatrixMultiply.class);

  job2.setMapperClass(MatrixMapper2.class);
  //job2.setCombinerClass(MatrixReducer2.class);
  job2.setReducerClass(MatrixReducer2.class);
  //job2.setMapOutputKeyClass(Text.class);
  //job2.setMapOutputValueClass(IntWritable.class);
  job2.setOutputKeyClass(Text.class);
  job2.setOutputValueClass(IntWritable.class);

  job2.setInputFormatClass(TextInputFormat.class);
  job2.setOutputFormatClass(TextOutputFormat.class);

  TextInputFormat.addInputPath(job2, new Path("temp"));
  TextOutputFormat.setOutputPath(job2, new Path("output"));
  
  System.exit(job2.waitForCompletion(true) ? 0 : 1);
}
}
