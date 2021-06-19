package query1;

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
import org.javatuples.Sextet;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountBolt extends BaseRichBolt {
    static final String[] latID = {"D","F","G","H","I","J"};
    static final String[] ship_types ={"militare","passeggeri","cargo","other"};
    OutputCollector collector;

    /*
    * HashMap che ha
    * key = ship_type
    * value = anno , mese , giorno , settore , somma
    * */
    Map<Pair<String,String>, Quartet<String,String,String,Integer>> counts = new HashMap<Pair<String,String>, Quartet<String,String,String,Integer>>();
    /*
    HashMap che ha
    key = Settore
    Value = List<Ship-id>
     */
    Map<String, ArrayList<String>> presents = new HashMap<String, ArrayList<String>>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        /*
        Popoliamo l'HashMap dei settori
         */
        for(String lat : latID){
            for(Integer lon =1;lon <=17;lon++){
                String sector_id = lat + lon;
                presents.put(sector_id,new ArrayList<String>());
                for (String current_type : ship_types){
                    counts.put(new Pair<String,String>(current_type,sector_id),new Quartet<String,String,String,Integer>("15",
                            "03","10",0));
                }
            }
        }


        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sector_id = tuple.getString(4);
        String ship_type = tuple.getString(2);
        String ship_id =  tuple.getString(1);
        String date = tuple.getString(3);
        Pair<String,String> key = new Pair<String,String>(ship_type,sector_id);
        String[] date_splitted = date.substring(0,8).split("-");

        /**System.out.println("La chiave è presente ?  "+ counts.containsKey(key));

        if(counts.containsKey(key)){
            System.out.println("++++++++++++++++++++++++++++++++++++++\n\n\n");
            System.out.println("ANNO ATTUALE "+counts.get(key).getValue0()+"   ANNO TUPLA  "+date_splitted[0]);
            System.out.println("MESE ATTUALE "+counts.get(key).getValue1()+"   MESE TUPLA  "+date_splitted[1]);
            System.out.println("GIORNO ATTUALE "+counts.get(key).getValue2()+"   GIORNO TUPLA  "+date_splitted[2]);
            System.out.println(" VALORE CONDIZIONI  "+(!counts.get(key).getValue0().equals(date_splitted[0])
                    || !counts.get(key).getValue1().equals(date_splitted[1]) || !counts.get(key).getValue2().equals(date_splitted[2])));
        }**/
        if(counts.containsKey(key) && (!counts.get(key).getValue0().equals(date_splitted[0])
                || !counts.get(key).getValue1().equals(date_splitted[1]) || !counts.get(key).getValue2().equals(date_splitted[2]))){
            //EMIT
            //System.out.println("PRINT EMIT\n\n\n\n***************************************\n\n\n");
            reset_map(date_splitted);

        }

        if(!presents.get(sector_id).contains(ship_id)){
            //System.out.println("PRINT ADD\n\n\n\n#########################################\n\n\n");
            presents.get(sector_id).add(ship_id);
            Integer previus_count = counts.get(key).getValue3();
            Quartet<String,String,String,Integer> new_quartet = counts.get(key).setAt3(previus_count+1);
            //System.out.println("QUARTETTO "+new_quartet+"            \n");
            counts.put(key,new_quartet);
        }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data","sector_id","ship_type","number"));
    }

    public void reset_map(String[] date_splitted){
        System.out.println("**********************NUOVO EMIT\n\n**********************");

        for(Pair<String,String> current_key :counts.keySet()){

            /**System.out.print("DATA   "+counts.get(key).getValue0()+"/"+counts.get(key).getValue1()+"/"+counts.get(key).getValue2());
            System.out.print("     SETTORE "+key.getValue1()+"   TIPO NAVE  "+key.getValue0());
            System.out.print("    NUMERO NAVI    "+counts.get(key).getValue3()+"\n");**/

            String data = counts.get(current_key).getValue0()+"-"+counts.get(current_key).getValue1()+"-"+counts.get(current_key).getValue2();
            String cell = current_key.getValue1();
            String type_n = current_key.getValue0();
            String num_n = counts.get(current_key).getValue3().toString();
            System.out.println("DATA : " + data + "   SETTORE :  " + cell + "    TIPO : " + type_n + "     VALORE : " + num_n);

            collector.emit(new Values(data,cell,type_n,num_n));
            counts.put(current_key,new Quartet<String,String,String,Integer>(date_splitted[0],
                    date_splitted[1],date_splitted[2],0));
            presents.put(current_key.getValue1(), new ArrayList<String>());
        }
    }
}
