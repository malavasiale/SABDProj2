package query1;

import org.apache.storm.metric.api.AssignableMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Classe che permette di sommare i dati di un determinato settore e per ogni tipologia di nave per ogni settimana o mese
 */
public class SumBolt1 extends BaseRichBolt {
    AssignableMetric latency ;
    OutputCollector collector;
    /*
    * HashMap con :
    * key = settore , timestamp iniziale, timestamp limite
    * value = List<Pair<tipo nave , numero>>
    * */
    Map<Triplet<String,Long,Long>, ArrayList<Pair<String,Integer>>> days_counts = new HashMap<Triplet<String,Long,Long>,ArrayList<Pair<String,Integer>>>();

    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd");
    static final String[] ship_types ={"militare","passeggeri","cargo","other"};
    Integer size_for_mode;
    long millis_mode;
    long start;

    Integer days_for_mode;

    /**
     * Costruttore
     * @param mode
     */
    public SumBolt1(String mode){
        if(mode.equals("week")){
            this.size_for_mode = 28;
            this.millis_mode = TimeUnit.DAYS.toMillis(7);
            this.days_for_mode = 7;
        }else{
            this.size_for_mode = 124;
            this.millis_mode = TimeUnit.DAYS.toMillis(31);
            this.days_for_mode = 31;
        }

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        latency = new AssignableMetric(new Long(0));
        start= 0;
        topologyContext.registerMetric("Latency-sum",latency,10);
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(start == 0){
            start = System.nanoTime();
        }
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
        //Prendere tutte le chiavi presenti nell'HashMap
        for(Triplet<String,Long,Long> key : days_counts.keySet()){
            //Se il sector_id è già presente e il timestamp sta nel range, allora aggiungo il valore alla lista
            if(key.getValue0().equals(sector_id) && timestamp >= key.getValue1() && timestamp < key.getValue2()){
                found = true;
                days_counts.get(key).add(new Pair<String,Integer>(ship_type,count));

                /*Calcolo ed emit tupla finale*/
                //Controllo per verificare che tutti i dati di una settimana o di un mese sono arrivati per il calcolo della media
                if(days_counts.get(key).size() == size_for_mode){
                    ArrayList<Pair<String,Integer>> to_scroll = days_counts.get(key);
                    Date d = new Date(key.getValue1());
                    this.format.format(d);
                    String row = d + "," + key.getValue0();
                    for(String type : ship_types){
                        Integer sum = 0;
                        for(Pair<String,Integer> elem : to_scroll){
                            if(elem.getValue0().equals(type)){
                                sum = sum + elem.getValue1();
                            }
                        }
                        Double mean = (sum/ days_for_mode)*1.0;
                        row = row +","+ type + "," + mean;
                    }
                    long end = System.nanoTime();
                    latency.setValue(new Long((end-start)));
                    start = 0;
                    collector.emit(new Values(row));
                    days_counts.remove(key);
                }

                break;
            }
        }
        /*Se non ho trovato dove inserire la tupla, creo una nuova entry nell HashMap*/
        if(!found){
            Triplet<String,Long,Long> new_key = new Triplet<String,Long,Long>(sector_id,timestamp,timestamp+ millis_mode);
            ArrayList<Pair<String,Integer>> new_list = new ArrayList<Pair<String,Integer>>();
            new_list.add(new Pair<String,Integer>(ship_type,count));
            days_counts.put(new_key,new_list);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row"));
    }
}
