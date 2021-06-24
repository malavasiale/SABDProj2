package query3;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.javatuples.Pair;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PartialRanckBolt3 extends BaseRichBolt {
    String aa = "timestamp trip_id distance";
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm");
    Long timestamp_start;
    List<Pair<String,Double>> array_window;
    OutputCollector outputCollector;

    private Integer intervallo_num;

    public PartialRanckBolt3(String intervallo){
        this.intervallo_num = Integer.parseInt(intervallo);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        array_window = new ArrayList<Pair<String,Double>>();
        try {
            Date date_start = format.parse("15-03-10 12:00");
            timestamp_start = date_start.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String trip_id = tuple.getString(1);
        Long windowStart = tuple.getLong(0);
        Double distance = tuple.getDouble(2);
        //System.out.println("TUPLA ARRIVATA "+tuple.toString()+"\n");
        if(windowStart > timestamp_start){
            if(!(array_window.size()==0) || !(array_window.size() ==1)){
                array_window.sort(new Comparator<Pair<String,Double>>() {
                    @Override
                    public int compare(Pair<String, Double> t0, Pair<String, Double> t1) {

                        return t1.getValue1().compareTo(t0.getValue1());
                    }
                });
            }
            //System.out.println("TIMESTAMP ARRAYLIST DA INVIARE "+ timestamp_start+"   "+array_window.toString()+"\n");
            outputCollector.emit(new Values(array_window,timestamp_start));
            timestamp_start = timestamp_start +TimeUnit.HOURS.toMillis(intervallo_num);
            array_window = new ArrayList<Pair<String,Double>>();
            while (windowStart > timestamp_start){
                //System.out.println("TIMESTAMP ARRAYLIST DA INVIARE "+ timestamp_start+"   "+array_window.toString()+"\n");
                outputCollector.emit(new Values(array_window,timestamp_start));
                timestamp_start = timestamp_start +TimeUnit.HOURS.toMillis(intervallo_num);
            }
        }

        Pair<String,Double> current_pair = new Pair<String,Double>(trip_id,distance);
        //System.out.println(current_pair.toString()+" Lista "+array_window.toString()+"\n");
        array_window.add(current_pair);




        //System.out.println("È ARRIVATO IL VIAGGIO "+trip_id+" APPERTENENTE ALLA FINESTRA CON TIME INIZIALE "+ windowStart+ " CON DISTANZA "+distance);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("array","timestamp"));
    }
}
