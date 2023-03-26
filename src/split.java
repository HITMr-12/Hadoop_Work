import java.io.*;

public class split {

    public void test() throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream("/mnt/hgfs/share/bigdata_analyze/data.txt") ));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/mnt/hgfs/share/bigdata_analyze/new_data.txt") ));
        String str;
        while ((str = in.readLine()) != null) {
            String[] info = str.trim().split(",");
            if(info[0].matches("^[0-9]$") && Integer.parseInt(info[0]) == 1){
                out.write(str + "\n");
            }
            else {
                break;
            }
        }
        out.flush();
        in.close();
        out.close();
    }
}
