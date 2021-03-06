package query2;

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


import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Classe che permette di ordinare i settori in base al numero di navi diverse presenti nelle varie ore
 */
public class RankBolt2 extends BaseRichBolt {
    long start;
    AssignableMetric latency;
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd");
    /*
    HashMap :
        -Key : TimeStamp, mare, fascia
        -Value : arrayList<Pair<settore,totale>
     */
    HashMap<Triplet<Long,String,String>, ArrayList<Pair<String,Integer>>> collect =  new HashMap<Triplet<Long,String,String>, ArrayList<Pair<String,Integer>>>();
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        
        //Inizializzo metrica latenza personalizzata
        latency = new AssignableMetric(new Long(0));
        start= 0;
        topologyContext.registerMetric("Latency-rank",latency,10);
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(start == 0){
            start = System.nanoTime();
        }
        long timestamp = tuple.getLong(0);
        String fascia = tuple.getString(1);
        String sector_id = tuple.getString(2);
        String sea = tuple.getString(3);
        Integer total = tuple.getInteger(4);
        Triplet<Long,String,String> current_key = new Triplet<Long,String,String>(timestamp,sea,fascia);
        //Controllo se non è stato ancora inserito nessun dato nell'HashMap
        if(collect.get(current_key) !=null){
            collect.get(current_key).add(new Pair<String,Integer>(sector_id,total));
        }else{
            //Aggiornamento lista
            ArrayList<Pair<String,Integer>> current_list = new ArrayList<Pair<String,Integer>>();
            current_list.add(new Pair<String,Integer>(sector_id,total));
            collect.put(current_key,current_list);
        }
        if(sea.equals("occidentale")){
            String other_fascia = "";
            if(fascia.equals("prima")){
                other_fascia = "seconda";
            }else{
                other_fascia = "prima";
            }
            Triplet<Long,String,String> other_key = new Triplet<Long,String,String>(timestamp,sea,other_fascia);
            //Controllo arrivo di tutti i dati
            if(collect.get(current_key).size() == 170 && collect.get(other_key).size() == 170){
                //Ordinamento rispetto al totale
                collect.get(current_key).sort(new Comparator<Pair<String,Integer>>() {
                    @Override
                    public int compare(Pair<String, Integer> t0, Pair<String, Integer> t1) {
                        if (t0.getValue1() > t1.getValue1()) {
                            return -1;
                        } else if (t0.getValue1()==t1.getValue1()) {
                            return 0;
                        } else {
                            return 1;
                        }
                    }

                });
                collect.get(other_key).sort(new Comparator<Pair<String,Integer>>() {
                    @Override
                    public int compare(Pair<String, Integer> t0, Pair<String, Integer> t1) {
                        if (t0.getValue1() > t1.getValue1()) {
                            return -1;
                        } else if (t0.getValue1()==t1.getValue1()) {
                            return 0;
                        } else {
                            return 1;
                        }
                    }

                });
                Date d = new Date(current_key.getValue0());
                this.format.format(d);
                //Creazione riga per emissione su Rabbit
                String row = "";
                if(other_fascia.equals("seconda")){
                    row = d+","+sea+","+fascia+","+collect.get(current_key).get(0).getValue0()+"--"+collect.get(current_key).get(1).getValue0()+ "--"+collect.get(current_key).get(2).getValue0()+","+
                            other_fascia+","+collect.get(other_key).get(0).getValue0()+"--"+collect.get(other_key).get(1).getValue0()+ "--"+collect.get(other_key).get(2).getValue0();
                }else{
                    row = d+","+sea+","+other_fascia+","+collect.get(other_key).get(0).getValue0()+"--"+collect.get(other_key).get(1).getValue0()+ "--"+collect.get(other_key).get(2).getValue0()+","+
                            fascia+","+collect.get(current_key).get(0).getValue0()+"--"+collect.get(current_key).get(1).getValue0()+ "--"+collect.get(current_key).get(2).getValue0();
                }
                long end = System.nanoTime();
                latency.setValue(new Long(end-start));
                start = 0;
                collector.emit(new Values(row));
                collect.remove(current_key);
                collect.remove(other_key);
            }
        }else{ // Uguale al codice dell if ma per il mare orientale
            String other_fascia = "";
            if(fascia.equals("prima")){
                other_fascia = "seconda";
            }else{
                other_fascia = "prima";
            }
            Triplet<Long,String,String> other_key = new Triplet<Long,String,String>(timestamp,sea,other_fascia);
            if(collect.get(current_key).size() == 230 && collect.get(other_key).size() == 230){
                collect.get(current_key).sort(new Comparator<Pair<String,Integer>>() {
                    @Override
                    public int compare(Pair<String, Integer> t0, Pair<String, Integer> t1) {
                        if (t0.getValue1() > t1.getValue1()) {
                            return -1;
                        } else if (t0.getValue1()==t1.getValue1()) {
                            return 0;
                        } else {
                            return 1;
                        }
                    }

                });
                collect.get(other_key).sort(new Comparator<Pair<String,Integer>>() {
                    @Override
                    public int compare(Pair<String, Integer> t0, Pair<String, Integer> t1) {
                        if (t0.getValue1() > t1.getValue1()) {
                            return -1;
                        } else if (t0.getValue1()==t1.getValue1()) {
                            return 0; 
                        } else {
                            return 1;
                        }
                    }

                });
                Date d = new Date(current_key.getValue0());
                this.format.format(d);
                String row = "";
                if(other_fascia.equals("seconda")){
                    row = d+","+sea+","+fascia+","+collect.get(current_key).get(0).getValue0()+"--"+collect.get(current_key).get(1).getValue0()+ "--"+collect.get(current_key).get(2).getValue0()+","+
                            other_fascia+","+collect.get(other_key).get(0).getValue0()+"--"+collect.get(other_key).get(1).getValue0()+ "--"+collect.get(other_key).get(2).getValue0();
                }else{
                    row = d+","+sea+","+other_fascia+","+collect.get(other_key).get(0).getValue0()+"--"+collect.get(other_key).get(1).getValue0()+ "--"+collect.get(other_key).get(2).getValue0()+","+
                            fascia+","+collect.get(current_key).get(0).getValue0()+"--"+collect.get(current_key).get(1).getValue0()+ "--"+collect.get(current_key).get(2).getValue0();
                }
                long end = System.nanoTime();
                latency.setValue(new Long(end-start));
                start = 0;
                collector.emit(new Values(row));
                collect.remove(current_key);
                collect.remove(other_key);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row"));
    }
}
