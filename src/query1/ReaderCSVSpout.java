package query1;

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
import java.util.Map;

public class ReaderCSVSpout extends BaseRichSpout {
    private FileReader filereader;
    private CSVReader csvReader;
    private SpoutOutputCollector _collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        try {
            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
            filereader = new FileReader("/data/dataset_sorted.csv");
            csvReader = new CSVReaderBuilder(filereader)
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
                _collector.emit(new Values(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field1","field2","fiend3","filed4",
                "filed5","field6","field7","filed8","field9","field10","field11"));
    }
}