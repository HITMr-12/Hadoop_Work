import com.sun.org.slf4j.internal.LoggerFactory;
import com.sun.org.slf4j.internal.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

public class Normalize {
    public static class NormalizeMapper extends Mapper<LongWritable, Text,Text, Text> {
        private static final Logger logger = LoggerFactory.getLogger(NormalizeMapper.class);
        private Date LegalFormatJudge(String dateString, SimpleDateFormat simpleDateFormat){
            try{
                return simpleDateFormat.parse(dateString);
            }catch (Exception e){
                return null;
            }
        }

        public String NormalizeDate(String dateString){
            SimpleDateFormat normalDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy/MM/dd");
            SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("MMM d,yyyy", Locale.US);
            Date date;
            if((date=LegalFormatJudge(dateString,simpleDateFormat1))!=null){
                return normalDateFormat.format(date);
            }
            else if((date=LegalFormatJudge(dateString,simpleDateFormat2))!=null){
                return normalDateFormat.format(date);
            }
            else if(dateString.contains(",")){
                logger.error("parse error:"+dateString);
            }
            return dateString;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 读取一条数据
            String line = value.toString();
            // 划分列
            String[] eles = line.split("\\|");
            logger.debug("befor:"+line);
            // 标准化温度
            String temperature = eles[5];
            if(temperature.contains("℉")){
                eles[5]= String.format("%.1f",(Double.parseDouble(temperature.replaceAll("℉","")) - 32) / 1.8) +"℃";
            }
            // 标准化日期
            eles[4] = NormalizeDate(eles[4]);
            eles[8] = NormalizeDate(eles[8]);
            String v = StringUtils.join("|",eles);
            logger.debug("value_after:"+v);
            context.write(new Text(""),new Text(v));
        }
    }
    public static class NormalizeReduce extends Reducer<Text,Text,Text,Text> {
        double max_rating=Double.NEGATIVE_INFINITY;
        double min_rating=Double.POSITIVE_INFINITY;
        private static final Logger logger = LoggerFactory.getLogger(NormalizeReduce.class);

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            MarkableIterator<Text> mitr = new MarkableIterator<>(values.iterator());
            mitr.mark();
            // 记录min，max值
            for(Text value:values) {
                // 读取一条数据
                String line = value.toString();
                // logger.warn(line);
                // 划分列
                String[] eles = line.split("\\|");
                try {
                    double rating = Double.parseDouble(eles[6]);
                    max_rating = Math.max(rating, max_rating);
                    min_rating = Math.min(rating, min_rating);
                } catch (Exception e){
                    logger.warn(Arrays.toString(e.getStackTrace()));
                }
            }
            mitr.reset();
            for(Text value:values){
                // 读取一条数据
                String line = value.toString();
                // 划分列
                String[] eles = line.split("\\|");
                try {
                    double rating = Double.parseDouble(eles[6]);
                    eles[6] = String.valueOf((rating - min_rating) / (max_rating - min_rating));
                }catch (Exception ignored){

                }
                context.write(new Text(""),new Text(StringUtils.join("|", eles)));
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String inputPath="hdfs://localhost:9000/lab1/D_Filter/D_Filter";
        String outputURI = "hdfs://localhost:9000/lab1/D_Normalize";
        String fileName = "D_Filter";
        Path outputPath= new Path(outputURI);
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "");
        FileSystem fs = FileSystem.get(new URI(outputURI),conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        Job job = Job.getInstance(conf, Normalize.class.getSimpleName());
        job.setJarByClass(Normalize.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(Normalize.NormalizeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Normalize.NormalizeReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MyTextOutputFormat.SetFileName(fileName);
        job.setOutputFormatClass(MyTextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        fs.close();
    }
}
