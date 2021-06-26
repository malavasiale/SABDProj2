package query3;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
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
    private FileReader filereader;
    private CSVReader csvReader;
    private SpoutOutputCollector _collector;
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm");
    private Date date_start;
    private long timestamp_start;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        try {
            date_start = format.parse("15-03-10 12:00");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        try {
            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(',')
                    .build();
            filereader = new FileReader("/data/dataset_sorted.csv");
            csvReader = new CSVReaderBuilder(filereader)
                    .withMultilineLimit(2)
                    .withCSVParser(parser)
                    .build();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        String[] row;


        try {

            if((row = csvReader.readNext()) != null){

                //System.out.println("DATA CORRENTE "+d.toString()+"  DATA LIMITE "+date_start.toString()+" è successiva? "+d.after(date_start));
                Date d = format.parse(row[7]);
                long timestamp = d.getTime();
                //System.out.println("DATA CORRENTE "+d.toString()+"  DATA LIMITE "+date_start.toString()+" è successiva? "+d.after(date_start));
                if(d.after(date_start)){
                    //System.out.println("  \nSTO INVIANDO"+d.toString()+"\n");
                    _collector.emit(new Values(timestamp,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]));
                }


            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp","ship_id","ship_type","speed","lon",
                "lat","course","heading","date","depart_port","draught","trip_id"));
    }
}