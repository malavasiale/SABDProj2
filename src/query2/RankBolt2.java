package query2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.javatuples.Pair;
import org.javatuples.Triplet;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RankBolt2 extends BaseRichBolt {
    String ordine = "timestamp"+"fascia"+"sector_id"+"sea"+"total";
    /*
    HashMap :
        -Key : TimeStamp, mare, fascia
        -Value : arrayList<Pair<settore,totale>
     */
    HashMap<Triplet<Long,String,String>, ArrayList<Pair<String,Integer>>> collet =  new HashMap<Triplet<Long,String,String>, ArrayList<Pair<String,Integer>>>();
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long timestamp = tuple.getLong(0);
        String fascia = tuple.getString(1);
        String sector_id = tuple.getString(2);
        String sea = tuple.getString(3);
        Integer total = tuple.getInteger(4);
        Triplet<Long,String,String> current_key = new Triplet<Long,String,String>(timestamp,sea,fascia);
        if(collet.get(current_key) !=null){
            collet.get(current_key).add(new Pair<String,Integer>(sector_id,total));
        }else{
            ArrayList<Pair<String,Integer>> current_list = new ArrayList<Pair<String,Integer>>();
            current_list.add(new Pair<String,Integer>(sector_id,total));
            collet.put(current_key,current_list);
        }
        if(sea.equals("occidentale")){
            if(collet.get(current_key).size() == 170){
                //sort e emit
            }
        }else{
            if(collet.get(current_key).size() == 230){
                //sort e emit
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
