package query1;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Sextet;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {

    OutputCollector collector;

    /*
    * HashMap che ha
    * key = ship_type
    * value = anno , mese , giorno ,ship_id, settore , somma
    * */
    Map<String, Sextet<String,String,String,String,String,Integer>> counts = new HashMap<String, Sextet<String,String,String,String,String,Integer>>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String date = tuple.getString(3);
        String[] date_splitted = date.substring(0,7).split("-");
        if(!counts.containsKey(tuple.getString(2))){
            counts.put(tuple.getString(2),new Sextet<String,String,String,String,String,Integer>(date_splitted[0],
                    date_splitted[1],date_splitted[2],tuple.getString(1),tuple.getString(4),1));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
