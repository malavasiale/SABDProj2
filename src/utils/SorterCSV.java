package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class SorterCSV {

    public static void main(String[] args) throws IOException, ParseException {

        List<String[]> to_sort = new ArrayList<String[]>();
        FSDataInputStream inputStream = null;
        
        // Inizializzo parametri di configurazione per la scrittura diretta su HDFS
        Configuration conf = new Configuration();
        conf.addResource(new Path("/data/Hadoop/core-site.xml"));
        conf.addResource(new Path("/data/Hadoop/hdfs-site.xml"));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        conf.set("dfs.replication", "1");
        FileSystem fs;
        FileSystem fs1 = null;
        FSDataOutputStream outputStream = null;
        PrintWriter writer;
        try {
            fs = FileSystem.get(URI.create("hdfs://localhost:9000/input/prj2_dataset.csv"),conf);
            inputStream = fs.open(new Path("/input/prj2_dataset.csv"));
            fs1 = FileSystem.get(URI.create("hdfs://localhost:9000/input/dataset_sorted.csv"),conf);
            outputStream = fs1.create(new Path("/input/dataset_sorted.csv"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        SimpleDateFormat input_format1=new SimpleDateFormat("dd-MM-yy HH:mm");
        SimpleDateFormat input_format2=new SimpleDateFormat("dd/MM/yy HH:mm");

        SimpleDateFormat format2 = new SimpleDateFormat("yy-MM-dd HH:mm");
        Date date;

        // Leggo tutti i dati dal file e li inserisco in una lista
        String line;
        String[] row;
        inputStream.readLine();
        while((line = inputStream.readLine()) != null){
            row = line.split(",");
            
            // Modifica data dal formato dd-MM-yy HH:mm  in   yy-MM-dd HH:mm
            if(row[4].contains("-")){
                date = input_format1.parse(row[4]);
            }else{
                date = input_format2.parse(row[4]);
            }
            row[4] = format2.format(date);
            to_sort.add(row);
        }
        writer = new PrintWriter(outputStream);
        
        // Ordinamento lessicografico sul campo contenente la data
        to_sort.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] l1, String[] l2) {
                return l1[4].compareTo(l2[4]);
            }
        });
        
        // Scrittura in append su un file nell' HDFS
        for(int i=0; i< to_sort.size();i++){
            writer.append(to_sort.get(i)[0]+","+to_sort.get(i)[1]+","+to_sort.get(i)[2]+","+to_sort.get(i)[3]+","+to_sort.get(i)[4]+","+to_sort.get(i)[5]+"\n");
        }
        writer.close();
    }

}
