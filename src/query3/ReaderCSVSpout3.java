package query3;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Metodo per la lettura del file CSV con nella query1 & query2
 */
public class ReaderCSVSpout3 extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm");
    private Date date_start;
    private long timestamp_start;
    private FSDataInputStream inputStream;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        try {
            date_start = format.parse("15-03-15 00:00");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        timestamp_start = date_start.getTime();
        Configuration conf = new Configuration();
        conf.addResource(new Path("/data/Hadoop/core-site.xml"));
        conf.addResource(new Path("/data/Hadoop/hdfs-site.xml"));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            inputStream = fs.open(new Path("/input/dataset_sorted.csv"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void nextTuple() {
        String line;


        try {
            if ((line = inputStream.readLine())!=null){
                String[] splitted_line = line.split(",");
                String id = splitted_line[0];
                String type = splitted_line[1];
                String lon = splitted_line[2];
                String lat = splitted_line[3];
                String date = splitted_line[4];
                String trip_id = splitted_line[5];
                Date d = format.parse(splitted_line[4]);

                long timestamp = d.getTime();
                //Condizione che permette di inviare i dati dopo una certa data scelta
                if(d.after(date_start)){
                    _collector.emit(new Values(timestamp,id,type,lon,lat,date,trip_id));
                }
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp","ship_id","ship_type","lon","lat",
                "data","trip_id"));
    }
}