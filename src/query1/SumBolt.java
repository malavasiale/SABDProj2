package query1;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SumBolt extends BaseRichBolt {
    /*
    * HashMap con :
    * key = settore , timestamp iniziale, timestamp limite
    * value = List<Pair<tipo nave , numero>>
    * */
    Map<Triplet<String,Long,Long>, ArrayList<Pair<String,Integer>>> days_counts = new HashMap<Triplet<String,Long,Long>,ArrayList<Pair<String,Integer>>>();

    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd");
    Integer size_for_week = 28;
    long millis_week = 604800000;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String date = tuple.getString(0);
        String sector_id = tuple.getString(1);
        String ship_type = tuple.getString(2);
        Integer count = Integer.parseInt(tuple.getString(3));
        long timestamp = 0;
        boolean found = false;
        try {
            Date d = format.parse(date);
            timestamp = d.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        for(Triplet<String,Long,Long> key : days_counts.keySet()){
            /*Se il sector_id è già presente e il timestamp sta nel range, allora aggiungo il valore alla lista*/
            if(key.getValue0().equals(sector_id) && timestamp >= key.getValue1() && timestamp <= key.getValue2()){
                found = true;
                System.out.println("**********TROVATO NUOVO ELEMENTO DEL GIORNO  : " + key.toString() + "******************");
                days_counts.get(key).add(new Pair<String,Integer>(ship_type,count));
                if(days_counts.get(key).size() == size_for_week){
                    System.out.println("**********FINITO IL GIORNO : " + key.toString() + "******************");
                    //TODO: FAI LA MEDIA ED EMETTI LA TUPLA
                    days_counts.remove(key);
                }
                break;
            }
        }
        /*Se non ho trovato dove inserire la tupla, creo una nuova entry nell HashMap*/
        if(!found){
            Triplet<String,Long,Long> new_key = new Triplet<String,Long,Long>(sector_id,timestamp,timestamp+millis_week);
            ArrayList<Pair<String,Integer>> new_list = new ArrayList<Pair<String,Integer>>();
            new_list.add(new Pair<String,Integer>(ship_type,count));
            System.out.println("-------------AGGIUNTO NUOVO GIORNO CON KEY : "+new_key.toString()+"-------------------------");
            days_counts.put(new_key,new_list);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
