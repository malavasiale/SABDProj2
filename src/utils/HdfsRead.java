package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsRead {

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path("/input/prj2_dataset.csv"));
        System.out.println(inputStream.readLine());
        String line;
        while ((line = inputStream.readLine())!=null){
            System.out.println(line);
            //TODO splittare con ? e prendere i vari campi
        }
    }
}
