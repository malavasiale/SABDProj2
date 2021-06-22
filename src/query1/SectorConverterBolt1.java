package query1;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import utils.ConvertToSector;

import java.util.Map;

public class SectorConverterBolt1 extends BaseWindowedBolt {
    OutputCollector collector;



    @Override
    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple t : tupleWindow.get()){
            String slat = t.getString(5);
            String slon = t.getString(4);

            String id = ConvertToSector.convertPointToSector(Double.parseDouble(slat),Double.parseDouble(slon));
            if(id.length()<5){
                if(ConvertToSector.isOccidental(id)){
                    //System.out.println("*************************************************\nData :"+t.getString(8));
                    String ship_type = ConvertToSector.shipType(t.getString(2));
                    collector.emit(new Values(t.getLong(0),t.getString(1),ship_type,t.getString(8),id));
                }
            }

        }

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp","ship_id","ship_type","data","id_sector"));
    }
}