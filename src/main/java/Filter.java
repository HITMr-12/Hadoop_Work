import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Filter {
    public static class FilterMapper extends Mapper<LongWritable,Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 读取一条数据
            String line = value.toString();
            // 划分列
            String[] eles = line.split("\\|");
            // 提取key
            double longitude = Double.parseDouble(eles[1]);
            double altitude = Double.parseDouble(eles[3]);
            if((longitude>=8.1461259&&longitude<=11.1993265)&&(altitude>=56.5824856&&altitude<=57.750511)){
                // 生成键值对
                context.write(new Text("key"),value);
            }

        }
    }
    public static class FilterReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text text:values){
                    context.write(new Text(""),text);
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String inputPath="hdfs://localhost:9000/lab1/D_Sample/D_Sample";
        String outputURI = "hdfs://localhost:9000/lab1/D_Filter";
        String fileName = "D_Filter";
        Path outputPath= new Path(outputURI);
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(outputURI),conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        Job job = Job.getInstance(conf, Filter.class.getSimpleName());
        job.setJarByClass(Filter.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(Filter.FilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Filter.FilterReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MyTextOutputFormat.SetFileName(fileName);
        job.setOutputFormatClass(MyTextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        fs.close();
    }
}
