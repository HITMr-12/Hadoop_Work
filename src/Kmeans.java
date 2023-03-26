import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;


public class Kmeans {
    private static String FLAG = "Kmeans";

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        double[][] centers = new double[Center.k][];
        String[] centerstrArray = null;

        @Override
        public void setup(Context context) throws InterruptedException {

            String kmeanss = context.getConfiguration().get(FLAG);
            //System.out.println("!"+ kmeanss);
            //Thread.sleep(5000);
            centerstrArray = kmeanss.split("\t");
            //System.out.println( centerstrArray);
            //Thread.sleep(10000);
            for(int i = 0; i < centerstrArray.length; i++) {
                String[] segs = centerstrArray[i].split(",");
                centers[i] = new double[segs.length];
                for(int j = 0; j < segs.length; j++) {
                    centers[i][j] = Double.parseDouble(segs[j]);
                }
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String[] segs = line.split(",");

            double[] sample = new double[3];
            sample[0] = (Double.parseDouble(segs[2]) - Center.min_WindSpeed) / (Center.max_WindSpeed - Center.min_WindSpeed);
            sample[1] = (Double.parseDouble(segs[3]) - Center.min_power) / (Center.max_power - Center.min_power);
            sample[2] = (Double.parseDouble(segs[4]) - Center.min_RotorSpeed) / (Center.max_RotorSpeed - Center.min_RotorSpeed);
            //求得距离最近的质心
            double min = Double.MAX_VALUE;
            int index = 0;
            for(int i = 0; i < centers.length; i++) {
                double dis = distance(centers[i], sample);
                if(dis < min) {
                    min = dis;
                    index = i;
                }
            }
            context.write(new Text(centerstrArray[index]), new Text(sample[0] + "," + sample[1] + "," + sample[2]));

        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text, NullWritable,Text> {

        Counter counter = null;

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            double[] sum = new double[3];
            int size = 0;
            //计算对应维度上值的加和，存放在sum数组中
            for(Text text : values) {
                String[] segs = text.toString().split(",");
                for(int i = 0; i < segs.length; i++) {
                    sum[i] += Double.parseDouble(segs[i]);
                }
                size ++;
            }

            //求sum数组中每个维度的平均值，也就是新的质心
            StringBuffer sb = new StringBuffer();

            for(int i = 0; i < sum.length; i++) {
                sum[i] /= size;
                sb.append(sum[i]);
                if(i != sum.length - 1){
                    sb.append(",");
                }
            }

            /**判断新的质心跟老的质心是否是一样的*/
            boolean flag = true;
            String[] centerStrArray = key.toString().split(",");
            for(int i = 0; i < centerStrArray.length; i++) {
                if(Math.abs(Double.parseDouble(centerStrArray[i]) - sum[i]) > 1e-10) {
                    flag = false;
                    break;
                }
            }
            counter = context.getCounter("Counter", "kmeansCounter");
            if(flag) {
                counter.increment(1);
            }
            //System.out.println("!" + sb.toString());
            //Thread.sleep(10000);
            context.write(null, new Text(sb.toString()));


            //long counter =  context.getCounter("myCounter", "setupCounter").getValue();
            //if(counter == 1)
                //context.write(null, new Text(String.valueOf(counter)));
        }

    }

    public static void main(String[] args) throws Exception {

        Path kMeansPath = new Path("/work/new_data.txt");	//初始的质心文件
        Path samplePath = new Path("/work/new_data.txt");	//样本文件
        //加载聚类中心文件
        //System.out.println(centerString);
        Center center = new Center();
        String centerString = center.loadInitCenter(kMeansPath);
        int index = 0;	//迭代的次数
        while(index < 10) {
            Configuration conf = new Configuration();
            conf.set(FLAG, centerString);

            kMeansPath = new Path("/work/output/output" + index);   


            FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(kMeansPath)) hdfs.delete(kMeansPath);

            Job job = new Job(conf, "kmeans" + index);
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, samplePath);
            FileOutputFormat.setOutputPath(job, kMeansPath);
            job.waitForCompletion(true);

            long counter = job.getCounters().findCounter("Counter", "kmeansCounter").getValue();
            if (counter == Center.k) {
                getResultFile(new Path("/lab2/kmeans/output/output" + index + "/part-r-00000"), samplePath);
                System.exit(0);
            }

            index++;
            center = new Center();
            centerString = center.loadCenter(kMeansPath);
        }

        index -= 1;
        getResultFile(new Path("/work/output/output" + index+ "/part-r-00000"), samplePath);
        System.exit(0);
        //split s = new split();
        //s.test();
    }

    public static void getResultFile(Path centerPath, Path samplePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path resultpath = new Path("/work/output/result");
        if(hdfs.exists(resultpath)) hdfs.delete(resultpath);
        FSDataOutputStream dos = hdfs.create(resultpath);
        FSDataInputStream cen = hdfs.open(centerPath);
        LineReader in = new LineReader(cen, conf);
        Text line = new Text();
        double[][] centers = new double[Center.k][3];
        int i = 0;
        while(in.readLine(line) > 0) {
            String[] segs = line.toString().split(",");
            for(int j = 0; j < segs.length; j++) {
                centers[i][j] = Double.parseDouble(segs[j]);
            }
            i++;
        }
        FSDataInputStream sam = hdfs.open(samplePath);
        LineReader samin = new LineReader(sam, conf);
        Text samline = new Text();
        int[] number = new int[3];
        String[] samlines = new String[50000];
        int cnt = 0;
        double rate1 = 0.1;
        double rate2 = 0.3;
        double[][] ans = new double[3][50000];
        int[][] pos = new int[3][50000];
        for(i = 0; i < 3; i++){
            for(int j = 0; j < ans[i].length; j++){
                ans[i][j] = Double.MAX_VALUE;
                pos[i][j] = Integer.MAX_VALUE;
            }
        }
        int temp1 = 0;
        while(samin.readLine(samline) > 0) {
            samlines[cnt++] = samline.toString();
            String[] segs = samline.toString().split(",");
            double[] sample = new double[3];
            sample[0] = (Double.parseDouble(segs[2]) - Center.min_WindSpeed) / (Center.max_WindSpeed - Center.min_WindSpeed);
            sample[1] = (Double.parseDouble(segs[3]) - Center.min_power) / (Center.max_power - Center.min_power);
            sample[2] = (Double.parseDouble(segs[4]) - Center.min_RotorSpeed) / (Center.max_RotorSpeed - Center.min_RotorSpeed);
            double min = Double.MAX_VALUE;
            int index = 0;
            double dis;
            for(int c = 0; c < centers.length; c++) {
                //System.out.println(centers[c]);
                dis = distance(centers[c], sample);
                //System.out.println(c);
                if(dis < min) {
                    min = dis;
                    index = c;
                }
            }
            number[index] += 1;
            int temp = ans[index].length;
            for(int j = ans[index].length - 1; j >= 0; j--){
                if(min < ans[index][j]){
                    temp = j;
                    if(j == 0){
                        for(int k = ans[index].length - 1; k > temp; k--){
                            ans[index][k] =  ans[index][k - 1];
                            pos[index][k] = pos[index][k - 1];
                        }
                        ans[index][temp] = min;
                        pos[index][temp] = cnt - 1;
                        break;
                    }
                }
                else if(temp < ans[index].length){
                    for(int k = ans[index].length - 1; k > temp; k--){
                        ans[index][k] =  ans[index][k - 1];
                        pos[index][k] = pos[index][k - 1];
                    }
                    ans[index][temp] = min;
                    pos[index][temp] = cnt - 1;
                    if(temp1 < temp)
                        temp1 = temp;
                    break;
                }
            }
        }
        System.out.println(number[0]);
        System.out.println(number[1]);
        System.out.println(number[2]);
        int biggest = 0;
        if(number[0] > number[1]){
            if(number[0] > number[2]){
            }
            else{
                biggest = 2;
            }
        }
        else{
            if(number[1] < number[2]){
                biggest  = 2;
            }
            else{
                biggest = 1;
            }
        }
        for(i = 0; i < ans.length; i++){
            if(i == biggest){
                for(int j = number[i] - 1; j > (int)(number[i] * (1 - rate1)); j--){
                    dos.write(samlines[pos[i][j]].getBytes(StandardCharsets.UTF_8));
                    dos.write("\n".getBytes(StandardCharsets.UTF_8));
                }
            }
            else{
                for(int j = number[i] - 1; j > (int)(number[i] * (1 - rate2)); j--){
                    dos.write(samlines[pos[i][j]].getBytes(StandardCharsets.UTF_8));
                    dos.write("\n".getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }

    public static double distance(double[] a, double[] b) {

        if(a == null || b == null || a.length != b.length) return Double.MAX_VALUE;
        double dis = 0;
        for(int i = 2; i < a.length; i++) {
            dis += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(dis);
    }
}
