import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.Random;


public class Center {

    protected static int k = 3;		//质心的个数
    static double max_WindSpeed = Double.MIN_VALUE;
    static double max_power = -Double.MAX_VALUE;
    static double max_RotorSpeed = Double.MIN_VALUE;
    static double min_WindSpeed = Double.MAX_VALUE;
    static double min_power = Double.MAX_VALUE;
    static double min_RotorSpeed = Double.MAX_VALUE;
    static double avg_WindSpeed = 0;
    static double avg_power = 0;
    static double avg_RotorSpeed = 0;
    public String loadInitCenter(Path path) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FSDataInputStream dis = hdfs.open(path);
        LineReader in = new LineReader(dis, conf);
        Text line = new Text();
        int cnt = 1;
        while(in.readLine(line) > 0) {
            String[] info  = line.toString().split(",");
            if(Double.parseDouble(info[2]) > max_WindSpeed){
                max_WindSpeed = Double.parseDouble(info[2]);
            }
            else if(Double.parseDouble(info[2]) < min_WindSpeed){
                min_WindSpeed = Double.parseDouble(info[2]);
            }
            if(Double.parseDouble(info[3]) > max_power){
                max_power = Double.parseDouble(info[3]);
            }
            else if(Double.parseDouble(info[3]) < min_power){
                min_power = Double.parseDouble(info[3]);
            }
            if(Double.parseDouble(info[4]) > max_RotorSpeed){
                max_RotorSpeed = Double.parseDouble(info[4]);
            }
            else if(Double.parseDouble(info[4]) < min_RotorSpeed){
                min_RotorSpeed= Double.parseDouble(info[4]);
            }
            if(cnt == 1) {
                avg_WindSpeed = Double.parseDouble(info[2]);
                avg_power = Double.parseDouble(info[3]);
                avg_RotorSpeed = Double.parseDouble(info[4]);
            }
            else{
                avg_WindSpeed = cnt * avg_WindSpeed / (cnt + 1);
                avg_power = cnt * avg_power / (cnt + 1);
                avg_RotorSpeed = cnt * avg_RotorSpeed / (cnt + 1);
            }
            cnt++;
        }
        in.close();
        Configuration new_conf = new Configuration();
        FileSystem new_hdfs = FileSystem.get(new_conf);
        FSDataInputStream new_dis = new_hdfs.open(path);
        LineReader inn = new LineReader(new_dis, conf);
        cnt = 1;
        String[] r = new String[k];
        while(inn.readLine(line) > 0) {
            if(cnt <= k){
                String[] info = line.toString().split(",");
                info[2] = String.valueOf((Double.parseDouble(info[2]) - min_WindSpeed) / (max_WindSpeed - min_WindSpeed));
                info[3] = String.valueOf((Double.parseDouble(info[3]) - min_power) / (max_power - min_power));
                info[4] = String.valueOf((Double.parseDouble(info[4]) - min_RotorSpeed) / (max_RotorSpeed - min_RotorSpeed));
                r[cnt - 1] = info[2] + ","+ info[3] + "," + info[4];
            }
            else{
                if(Math.random() < (double)k / cnt){
                    String[] info = line.toString().split(",");
                    info[2] = String.valueOf((Double.parseDouble(info[2]) - min_WindSpeed) / (max_WindSpeed - min_WindSpeed));
                    info[3] = String.valueOf((Double.parseDouble(info[3]) - min_power) / (max_power - min_power));
                    info[4] = String.valueOf((Double.parseDouble(info[4]) - min_RotorSpeed) / (max_RotorSpeed - min_RotorSpeed));
                    Random random = new Random();
                    r[random.nextInt(k)] = info[2] + ","+ info[3] + "," + info[4];
                }
            }
            cnt += 1;
        }
        for(int i = 0; i < k; i++){
            sb.append(r[i]);
            sb.append("\t");
        }
        return sb.toString().trim();
    }

    /**
     * 从每次迭代的质心文件中读取质心，并返回字符串
     * @param path
     * @return
     * @throws IOException
     */
    public String loadCenter(Path path) throws IOException {

        StringBuffer sb = new StringBuffer();

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] files = hdfs.listStatus(path);

        for(int i = 0; i < files.length; i++) {
            Path filePath = files[i].getPath();
            if(!filePath.getName().contains("part")) continue;
            FSDataInputStream dis = hdfs.open(filePath);
            LineReader in = new LineReader(dis, conf);
            Text line = new Text();
            while(in.readLine(line) > 0) {
                sb.append(line.toString().trim());
                sb.append("\t");
            }
        }

        return sb.toString().trim();
    }
}
