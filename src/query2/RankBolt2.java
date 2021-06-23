package query2;

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

public class RankBolt2 extends BaseRichBolt {
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd");
    String ordine = "timestamp"+"fascia"+"sector_id"+"sea"+"total";
    /*
    HashMap :
        -Key : TimeStamp, mare, fascia
        -Value : arrayList<Pair<settore,totale>
     */
    HashMap<Triplet<Long,String,String>, ArrayList<Pair<String,Integer>>> collect =  new HashMap<Triplet<Long,String,String>, ArrayList<Pair<String,Integer>>>();
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
        if(collect.get(current_key) !=null){
            collect.get(current_key).add(new Pair<String,Integer>(sector_id,total));
        }else{
            ArrayList<Pair<String,Integer>> current_list = new ArrayList<Pair<String,Integer>>();
            current_list.add(new Pair<String,Integer>(sector_id,total));
            collect.put(current_key,current_list);
        }
        if(sea.equals("occidentale")){
            if(collect.get(current_key).size() == 170){
                collect.get(current_key).sort(new Comparator<Pair<String,Integer>>() {
                    @Override
                    public int compare(Pair<String, Integer> t0, Pair<String, Integer> t1) {
                        if (t0.getValue1() > t1.getValue1()) {
                            return -1;
                        } else if (t0.getValue1()==t1.getValue1()) {
                            return 0; // You can change this to make it then look at the
                            //words alphabetical order
                        } else {
                            return 1;
                        }
                    }

                });
                Date d = new Date(current_key.getValue0());
                this.format.format(d);

                //System.out.println("Data Iniziale "+d+" Occidentale :\n"+"Fascia: "+fascia+"\n"+"Settori: "+collect.get(current_key).get(0).getValue0()+" "+collect.get(current_key).get(1).getValue0()+ " "+collect.get(current_key).get(2).getValue0()+" Con valori" +
                  //      " "+collect.get(current_key).get(0).getValue1()+" "+collect.get(current_key).get(1).getValue1()+" "+collect.get(current_key).get(2).getValue1()+"\n");
                String row = d+","+sea+","+fascia+","+collect.get(current_key).get(0).getValue0()+"--"+collect.get(current_key).get(1).getValue0()+ "--"+collect.get(current_key).get(2).getValue0()+","+collect.get(current_key).get(0).getValue1()+"--"+collect.get(current_key).get(1).getValue1()+"--"+collect.get(current_key).get(2).getValue1();
                collector.emit(new Values(row));
            }
        }else{
            if(collect.get(current_key).size() == 230){
                collect.get(current_key).sort(new Comparator<Pair<String,Integer>>() {
                    @Override
                    public int compare(Pair<String, Integer> t0, Pair<String, Integer> t1) {
                        if (t0.getValue1() > t1.getValue1()) {
                            return -1;
                        } else if (t0.getValue1()==t1.getValue1()) {
                            return 0; // You can change this to make it then look at the
                            //words alphabetical order
                        } else {
                            return 1;
                        }
                    }

                });
                Date d = new Date(current_key.getValue0());
                this.format.format(d);

                //System.out.println("Data Iniziale "+d+" Orientale :\n"+"Fascia: "+fascia+"\n"+"Settori: "+collect.get(current_key).get(0).getValue0()+" "+collect.get(current_key).get(1).getValue0()+ " "+collect.get(current_key).get(2).getValue0()+" Con valori" +
                //" "+collect.get(current_key).get(0).getValue1()+" "+collect.get(current_key).get(1).getValue1()+" "+collect.get(current_key).get(2).getValue1()+"\n");
                String row = d+","+sea+","+fascia+","+collect.get(current_key).get(0).getValue0()+"--"+collect.get(current_key).get(1).getValue0()+ "--"+collect.get(current_key).get(2).getValue0()+","+collect.get(current_key).get(0).getValue1()+"--"+collect.get(current_key).get(1).getValue1()+"--"+collect.get(current_key).get(2).getValue1();
                collector.emit(new Values(row));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row"));
    }
}
