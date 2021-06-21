package query2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.javatuples.Pair;
import org.javatuples.Quintet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CountBolt2  extends BaseRichBolt {
    static final String[] latID = {"A","B","C","D","E","F","G","H","I","J"};

    OutputCollector collector;

    /*
     * HashMap che ha
     * key = settore ,fascia
     * value = mare, anno, mese, giorno, somma
     * */
    Map<Pair<String,String>, Quintet<String,String,String,String,Integer>> counts = new HashMap<Pair<String,String>, Quintet<String,String,String,String,Integer>>();
    /*
    HashMap che ha
    key = Settore, fascia oraria
    Value = List<Ship-id>
     */
    Map<Pair<String,String>, ArrayList<String>> presents = new HashMap<Pair<String,String>, ArrayList<String>>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        /*
        Popoliamo l'HashMap dei settori
         */
        for(String lat : latID){
            for(Integer lon =1;lon <=17;lon++){
                String sector_id = lat + lon;
                presents.put(new Pair<String,String>(sector_id,"prima"),new ArrayList<String>());
                presents.put(new Pair<String,String>(sector_id,"seconda"),new ArrayList<String>());
                counts.put(new Pair<String,String>(sector_id,"prima"),new Quintet<String,String,String,String,Integer>("occidentale",
                            "15","03","15",0));
                counts.put(new Pair<String,String>(sector_id,"seconda"),new Quintet<String,String,String,String,Integer>("occidentale",
                        "15","03","15",0));


            }
        }
        for(String lat : latID){
            for(Integer lon =18;lon <=40;lon++){
                String sector_id = lat + lon;
                presents.put(new Pair<String,String>(sector_id,"prima"),new ArrayList<String>());
                presents.put(new Pair<String,String>(sector_id,"seconda"),new ArrayList<String>());
                counts.put(new Pair<String,String>(sector_id,"prima"),new Quintet<String,String,String,String,Integer>("orientale",
                        "15","03","15",0));
                counts.put(new Pair<String,String>(sector_id,"seconda"),new Quintet<String,String,String,String,Integer>("orientale",
                        "15","03","15",0));


            }
        }


        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sector_id = tuple.getString(4);
        String fascia = tuple.getString(2);
        String ship_id =  tuple.getString(1);
        String date = tuple.getString(3);
        String sea = tuple.getString(5);
        Pair<String,String> key = new Pair<String,String>(sector_id,fascia);
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
            try {
                reset_map(date_splitted);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

        if(!presents.get(key).contains(ship_id)){
            //System.out.println("PRINT ADD\n\n\n\n#########################################\n\n\n");
            presents.get(key).add(ship_id);
            Integer previus_count = counts.get(key).getValue4();
            Quintet<String,String,String,String,Integer> new_quartet = counts.get(key).setAt4(previus_count+1);
            //System.out.println("QUARTETTO "+new_quartet+"            \n");
            counts.put(key,new_quartet);
        }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data","sector_id","fascia","mare","number"));
    }

    public void reset_map(String[] date_splitted) throws ParseException {
        //System.out.println("**********************NUOVO EMIT\n\n**********************");

        for(Pair<String,String> current_key :counts.keySet()){

            /**System.out.print("DATA   "+counts.get(key).getValue0()+"/"+counts.get(key).getValue1()+"/"+counts.get(key).getValue2());
             System.out.print("     SETTORE "+key.getValue1()+"   TIPO NAVE  "+key.getValue0());
             System.out.print("    NUMERO NAVI    "+counts.get(key).getValue3()+"\n");**/

            String old_data = counts.get(current_key).getValue1()+"-"+counts.get(current_key).getValue2()+"-"+counts.get(current_key).getValue3();
            String cell = current_key.getValue0();
            String fascia = current_key.getValue1();
            String num_n = counts.get(current_key).getValue4().toString();
            String sea = counts.get(current_key).getValue0();
            long days_to_add = date_difference(old_data,date_splitted[0]+"-"+date_splitted[1]+"-"+date_splitted[2]);

            ;

            /*Se manca qualche giorno, faccio un emit del giorno vuoto*/
            if(days_to_add > 1){
                System.out.println("TROVATO GIORNI MANCANTI : " + old_data + " ----- " + date_splitted[1]+"-"+date_splitted[2]);
                for(int i = 1 ; i <= days_to_add-1;i++){
                    String date_to_add = emit_missing_days(old_data,i);
                    System.out.println("EMESSO IL SEGUENTE GIORNO VUOTO : " + date_to_add);
                    collector.emit(new Values(date_to_add,cell,fascia,sea,"0"));
                }
            }
            System.out.println("DATA : " + old_data + "   SETTORE :  " + cell + "    FASCIA : " + fascia +" MARE "+sea+"     VALORE : " + num_n+"\n");
            //collector.emit(new Values(old_data,cell,fascia,sea,num_n));
            counts.put(current_key,new Quintet<String,String,String,String,Integer>(sea,date_splitted[0],
                    date_splitted[1],date_splitted[2],0));
            presents.put(current_key, new ArrayList<String>());
        }
    }

    /*
     * Ritorna la differenza di giorni tra due date
     * */
    public long date_difference(String date1,String date2) throws ParseException {
        SimpleDateFormat date_format = new SimpleDateFormat("yy-MM-dd");

        Date d1 = date_format.parse(date1);
        Date d2 = date_format.parse(date2);

        long difference = d2.getTime() - d1.getTime();

        long difference_days
                = (difference
                / (1000 * 60 * 60 * 24))
                % 365;

        return difference_days;
    }

    public String emit_missing_days(String start_data,int days_to_add) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(start_data));
        c.add(Calendar.DATE, days_to_add);
        return sdf.format(c.getTime());
    }
}
