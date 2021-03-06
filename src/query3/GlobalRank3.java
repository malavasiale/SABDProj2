package query3;

import org.apache.storm.metric.api.AssignableMetric;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.javatuples.Pair;
import org.javatuples.Quintet;

import java.text.SimpleDateFormat;
import java.util.*;

public class GlobalRank3 extends BaseRichBolt {
    /*
    Bolt che permette di effettuare una classifica globale dete più classifiche parziali
    */
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm");
    private Integer num_of_partial = 3;
    long start;
    AssignableMetric latency;
    /*
    HASHMAP
        -key : timestamp (inizio finestra)
        -value : pair<ArrayList(trip_ide, distanza),liste arrivate
     */
    Map<Long, Pair<ArrayList<Pair<String,Double>>,Integer>> sorted_lists = new HashMap<Long, Pair<ArrayList<Pair<String,Double>>,Integer>>();
    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        /*
        Inizializzo metrica latenza personalizzata
        */
        latency = new AssignableMetric(new Long(0));
        start= 0;
        topologyContext.registerMetric("Latency-global",latency,10);
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(start == 0){
            start = System.nanoTime();
        }
        /*
        Inizializzazione variabili per lo svolgimento del lavoro del bolt
        */
        ArrayList<Pair<String,Double>> array = (ArrayList<Pair<String,Double>>)tuple.getValue(0);
        Long timestamp = tuple.getLong(1);
        Date d = new Date(timestamp);
        String date_string = format.format(d);
        
        //Controllo se tupla giò presente nell'HashMap
        if(!sorted_lists.containsKey(timestamp)){
            Pair<ArrayList<Pair<String,Double>>,Integer> current_pair = new Pair<ArrayList<Pair<String,Double>>,Integer>(array,1);
            sorted_lists.put(timestamp,current_pair);
        }else{
            //Inserimento nuovo elemento
            Pair<ArrayList<Pair<String,Double>>,Integer> current_pair = sorted_lists.get(timestamp);
            Pair<ArrayList<Pair<String,Double>>,Integer> current_pair_modified = current_pair.setAt1(current_pair.getValue1()+1);
            sorted_lists.put(timestamp,current_pair_modified);
            ArrayList<Pair<String,Double>> current_list = current_pair_modified.getValue0();

            current_list.addAll(array);
            List<Pair<String,Double>> new_list = new ArrayList<>(Sets.newLinkedHashSet(current_list));
            Integer num_list = current_pair_modified.getValue1();
            if(num_list.equals(num_of_partial)){ //il valore è unguale al numero di repliche di partiali rank
                new_list.sort(new Comparator<Pair<String,Double>>() {
                    @Override
                    public int compare(Pair<String, Double> t0, Pair<String, Double> t1) {

                        return t1.getValue1().compareTo(t0.getValue1());
                    }
                });
                String row = date_string;
                for(int i=0; i< new_list.size(); i++){
                    if(i==5){
                        break;
                    }
                    row = row+","+new_list.get(i).getValue0()+","+new_list.get(i).getValue1().toString();
                }
                //ordinamento tramite la funzione personalizzata sort
                long end = System.nanoTime();
                latency.setValue(new Long(end-start));
                start = 0;
                collector.emit(new Values(row));
                sorted_lists.remove(timestamp);
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row"));
    }
}
