import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Fill {



    public static class FillMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text result = new Text();
        private int k = 5;
        private List<Double[]> list_rating = new ArrayList<>();
        private Map<String,List<Double>> map_income = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("/home/hadoop/D_Filter"));
            String str;
            while((str=br.readLine())!=null){
                String[] itr =str.split("\\|");
                //鍒ゆ柇鏄惁涓虹┖
                if(!itr[11].equals("?")) {
                    double income = Double.valueOf(itr[11]);
                    double longitude = Double.valueOf(itr[1]);
                    double latitude = Double.valueOf(itr[2]);
                    double altitude = Double.valueOf(itr[3]);
                    if (!itr[6].equals("?")) {
                    	double rating = Double.valueOf(itr[6]);
                        String nation = itr[9];        
                        String career = itr[10];
                        Double[] d = {income, longitude, latitude, altitude, rating};
                        list_rating.add(d);
                        String s = nation + career;
                        //鑻ヤ笉鍖呭惈锛屽姞鍏ヨ褰�
                        if (!map_income.containsKey(s)) {
                            List<Double> l = new ArrayList<>();
                            l.add(income);
                            map_income.put(s, l);
                        } else {
                            List<Double> l = map_income.get(s);
                            l.add(income);
                        }
                    }
                }
            }
            System.out.println(list_rating.size());
            System.out.println(map_income.size());
        }

        private double distance(Double[] a,Double[] b){
            if(a.length!=b.length){
                return 0;
            }
            double sum = 0;
            for(int i=0;i<a.length-1;i++){
                sum+=Math.pow(a[i]-b[i],2);
            }
            return Math.sqrt(sum);
        }

        public void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] itr = value.toString().split("\\|");
            //绫籯nn绠楁硶锛屽彇鍊煎彇鍐充簬鏈�杩�5涓厓绱�
            if(itr[6].equals("?") && !itr[11].equals("?")){
                Double[] feature = {Double.valueOf(itr[11]),Double.valueOf(itr[1]),Double.valueOf(itr[2]),Double.valueOf(itr[3]),0.0};
                Double[] topk = new Double[k];
                int[] knn = new int[k];
                for(int i=0;i<k;i++){
                    topk[i]=Double.MAX_VALUE;
                }
                for(int i=0;i<list_rating.size();i++){
                    double dis = distance(feature,list_rating.get(i));
                    for(int j=0;j<k;j++){
                        if(dis<topk[j]){
                            topk[j]=dis;
                            knn[j]=i;
                            break;
                        }
                    }
                }
                double sum = 0.0;
                for(int index:knn){
                    sum+=list_rating.get(index)[4];
                }
                itr[6] = String.format("%.2f",sum/k);
            }
            //
            if(itr[11].equals("?")){
                String s = itr[9]+itr[10];
                double ans =0.0;
                if(map_income.containsKey(s)){
                    List<Double> list = map_income.get(s);
                    for(Double temp:list){
                        ans+=temp;
                    }
                    ans /=list.size();
                }
                itr[11] = String.format("%.0f", ans);
            }
            result.set(String.join("|",itr));
            context.write(NullWritable.get(),result);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException, URISyntaxException {
        String inputPath="hdfs://localhost:9000/lab1/D_Normalize/D_Filter";
        String outputURI = "hdfs://localhost:9000/lab1/D_Done";
        String fileName = "D_Done";
        Path outputPath= new Path(outputURI);
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        //BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "");
        FileSystem fs = FileSystem.get(new URI(outputURI),conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        Job job = Job.getInstance(conf, Fill.class.getSimpleName());
        job.setJarByClass(Fill.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(Fill.FillMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        MyTextOutputFormat.SetFileName(fileName);
        job.setOutputFormatClass(MyTextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        fs.close();
    }
}
