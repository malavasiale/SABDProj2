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
import org.javatuples.Quartet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Classe che permette di calcolare il numero di navi per ogni tipo presenti in ogni settore
 */
public class CountBolt1 extends BaseRichBolt {
    static final String[] latID = {"A","B","C","D","E","F","G","H","I","J"};
    static final String[] ship_types ={"militare","passeggeri","cargo","other"};
    boolean first;
    OutputCollector collector;
    /*
    * HashMap che ha
    * key = ship_type , settore
    * value = anno , mese , giorno , somma
    * */
    Map<Pair<String,String>, Quartet<String,String,String,Integer>> counts = new HashMap<Pair<String,String>, Quartet<String,String,String,Integer>>();
    /*
    HashMap che ha
    key = Settore
    Value = List<Ship-id>
     */
    Map<String, ArrayList<String>> presents = new HashMap<String, ArrayList<String>>();
    long start;
    AssignableMetric latency;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        /*
        Popoliamo l'HashMap dei settori
         */
        first = true;
        latency = new AssignableMetric(new Long(0));
        start= 0;
        topologyContext.registerMetric("Latency-count",latency,10);


        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(start == 0){
            start = System.nanoTime();
        }
        //Vengono presi tutti i dati utili per contare il numero di navi in un settore per un determinato tipo
        String sector_id = tuple.getString(4);
        String ship_type = tuple.getString(2);
        String ship_id =  tuple.getString(1);
        String date = tuple.getString(3);
        Pair<String,String> key = new Pair<String,String>(ship_type,sector_id);
        String[] date_splitted = date.substring(0,8).split("-");
        if(first){
            isFirst(date_splitted[0],date_splitted[1],date_splitted[2]);
            first = false;
        }
        //Controllo se è già arrivata una tupla con lo stesso tipo di nave e stesso settore
        //Ma con giorno diverso
        if(counts.containsKey(key) && (!counts.get(key).getValue0().equals(date_splitted[0])
                || !counts.get(key).getValue1().equals(date_splitted[1]) || !counts.get(key).getValue2().equals(date_splitted[2]))){
            try {
                reset_map(date_splitted);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

        //Controllo se una nave è già stata assegnata ad un settore
        if(!presents.get(sector_id).contains(ship_id)){
            //Aggiunta nave al settore
            presents.get(sector_id).add(ship_id);
            Integer previus_count = counts.get(key).getValue3();
            Quartet<String,String,String,Integer> new_quartet = counts.get(key).setAt3(previus_count+1);
            counts.put(key,new_quartet);
        }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data","sector_id","ship_type","number"));
    }

    public void reset_map(String[] date_splitted) throws ParseException {
        //Metodo per reset l'HashMap ed emettere i dati di un giorno
        for(Pair<String,String> current_key :counts.keySet()){
            //Valori utili per il controllo e l'emit
            String old_data = counts.get(current_key).getValue0()+"-"+counts.get(current_key).getValue1()+"-"+counts.get(current_key).getValue2();
            String cell = current_key.getValue1();
            String type_n = current_key.getValue0();
            String num_n = counts.get(current_key).getValue3().toString();
            /**
            long days_to_add = date_difference(old_data,date_splitted[0]+"-"+date_splitted[1]+"-"+date_splitted[2]);
            //System.out.println("DATA : " + data + "   SETTORE :  " + cell + "    TIPO : " + type_n + "     VALORE : " + num_n);
            if(days_to_add > 1){
                System.out.println("TROVATO GIORNI MANCANTI : " + old_data + " ----- " + date_splitted[1]+"-"+date_splitted[2]);
                for(int i = 1 ; i <= days_to_add-1;i++){
                    String date_to_add = emit_missing_days(old_data,i);
                    System.out.println("EMESSO IL SEGUENTE GIORNO VUOTO : " + date_to_add);
                    collector.emit(new Values(date_to_add,cell,type_n,"0"));
                }
            }**/
            long end = System.nanoTime();
            latency.setValue(new Long((end-start)));
            collector.emit(new Values(old_data,cell,type_n,num_n));
            counts.put(current_key,new Quartet<String,String,String,Integer>(date_splitted[0],
                    date_splitted[1],date_splitted[2],0));
            presents.put(current_key.getValue1(), new ArrayList<String>());
        }
        start=0;
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

    /**
     * Metodo per emetterre giorni mancanti
     * @param start_data
     * @param days_to_add
     * @return String
     * @throws ParseException
     */
    public String emit_missing_days(String start_data,int days_to_add) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(start_data));
        c.add(Calendar.DATE, days_to_add);
        return sdf.format(c.getTime());
    }

    /**
     * Metodo per inizializzare i settori dei Mar Mediterraneo
     * @param year
     * @param month
     * @param day
     */
    public void isFirst(String year,String month, String day){

        for(String lat : latID){
            for(Integer lon =1;lon <=17;lon++){
                String sector_id_init = lat + lon;
                presents.put(sector_id_init,new ArrayList<String>());
                for (String current_type : ship_types){
                    counts.put(new Pair<String,String>(current_type,sector_id_init),new Quartet<String,String,String,Integer>(year,
                            month,day,0));
                }
            }
        }

    }
}
