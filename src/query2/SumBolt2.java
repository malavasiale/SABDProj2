package query2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SumBolt2 extends BaseRichBolt {

    OutputCollector collector;
    /*
    * HashMap con :
    * key = settore , timestamp iniziale, timestamp limite, Fascia, mare
    * value = Pair<numero navi, numero tuple>
    * */
    Map<Quintet<String, Long, Long,String,String>, Pair<Integer,Integer>> days_counts = new HashMap<Quintet<String, Long, Long,String,String>, Pair<Integer,Integer>>();

    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd");
    Integer size_for_mode;
    long millis_mode;

    /*TODO: inizializzare a seconda se settimanale o mensile*/

    public SumBolt2(String mode){
        if(mode.equals("week")){
            this.size_for_mode = 7;
            this.millis_mode = TimeUnit.DAYS.toMillis(7);
        }else{
            this.size_for_mode = 30;
            this.millis_mode = TimeUnit.DAYS.toMillis(30);
        }

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String date = tuple.getString(0);

        String sector_id = tuple.getString(1);
        String fascia = tuple.getString(2);
        String sea = tuple.getString(3);
        Integer count = Integer.parseInt(tuple.getString(4));

        long timestamp = 0;
        boolean found = false;
        try {
            Date d = format.parse(date);
            timestamp = d.getTime();
            //System.out.println(date+" "+timestamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        for(Quintet<String,Long,Long,String,String> key : days_counts.keySet()){
            /*Se il sector_id è già presente e il timestamp sta nel range, allora aggiungo il valore alla lista*/

            if(key.getValue0().equals(sector_id) && key.getValue3().equals(fascia) && timestamp >= key.getValue1() && timestamp < key.getValue2()){

                found = true;

                Integer old_number = days_counts.get(key).getValue0();
                Integer num_tuple = days_counts.get(key).getValue1();
                days_counts.put(key,new Pair<Integer,Integer>(old_number+count,num_tuple+1));

                //System.out.println("AGGIUNTO NUOVO ELEMENTO ALLA SETTIMANA "+key+"-- LISTA :"+ days_counts.get(key).toString()+" IL SETTORE ARRIVATO È "+sector_id+" FASCIA ARRIVATA È "+fascia);

                /*Calcolo ed emit tupla finale*/
                if(days_counts.get(key).getValue1().equals(size_for_mode)){
                    System.out.println("FINITA SETTIMANA "+timestamp+ "   FASCIA "+fascia+"   SECTOR_ID "+sector_id+"   MARE "+sea+"  NUM "+days_counts.get(key).getValue0()+" ULTIMA DATA"+date);// modificare

                    collector.emit(new Values(timestamp,fascia,sector_id,sea,days_counts.get(key).getValue0()));
                    days_counts.remove(key);
                }
                break;
            }
        }
        /*Se non ho trovato dove inserire la tupla, creo una nuova entry nell HashMap*/
        if(!found){

            Quintet<String,Long,Long,String,String> new_key = new Quintet<String,Long,Long,String,String>(sector_id,timestamp,timestamp+ millis_mode,fascia,sea);
            //System.out.println("-------------AGGIUNTO NUOVA SETTIMANA CON KEY : "+date+"-------------------------"+new_key.toString());
            days_counts.put(new_key,new Pair<Integer,Integer>(count,1));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp","fascia","sector_id","sea","total"));
    }
}
