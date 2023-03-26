import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Sampling {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] eles = line.split("\\|");
            String k = eles[10];
            context.write(new Text(k),value);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text>{
        double samplingRate=0.1;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 执行抽样算法
            for(Text text:values){
                // 生成[0,1)随机数
                double rand = Math.random();
                // 执行抽样
                if(rand<=samplingRate){
                    context.write(new Text(""),text);
                }
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String inputPath="hdfs://localhost:9000/lab1/data.txt";
        String outputURI = "hdfs://localhost:9000/lab1/D_Sample";
        Path outputPath= new Path(outputURI);
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Configuration conf = new Configuration();
        //conf.set("mapred.textoutputformat.separator", "");
        FileSystem fs = FileSystem.get(new URI(outputURI),conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        Job job = Job.getInstance(conf,Sampling.class.getSimpleName());
        job.setJarByClass(Sampling.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MyTextOutputFormat.SetFileName("D_Sample");
        job.setOutputFormatClass(MyTextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        fs.close();
    }
}
