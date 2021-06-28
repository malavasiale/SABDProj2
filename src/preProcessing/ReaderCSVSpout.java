package preProcessing;

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


/**
 * Classe utilizzata per legge il file CSV
 */
public class ReaderCSVSpout extends BaseRichSpout {
    private FileReader filereader;
    private CSVReader csvReader;
    private SpoutOutputCollector _collector;



    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        try {
            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
            filereader = new FileReader("../data/prj2_dataset.csv");
            csvReader = new CSVReaderBuilder(filereader)
                    .withCSVParser(parser)
                    .withSkipLines(1)
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
                _collector.emit(new Values(row[0],row[1],row[2],row[3],row[4],row[5]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ship_id","ship_type","lon","lat",
                "timestamp","trip_id"));
    }
}
