package query3;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.javatuples.Pair;
import org.javatuples.Quintet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PartialRanckBolt3 extends BaseRichBolt {
    String aa = "timestamp trip_id distance";
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm");
    Long timestamp_start;
    ArrayList<Pair<String,Double>> array_window;

    OutputCollector outputCollector;
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
        String trip_id = tuple.getString(0);
        Long windowStart = tuple.getLong(1);
        Double distance = tuple.getDouble(2);
        if(windowStart > timestamp_start){
            //TODO ORDINARE, EMETTERE,
            timestamp_start = timestamp_start +TimeUnit.HOURS.toMillis(1);
            array_window.clear();
            while (windowStart > timestamp_start){
                //TODO EMIT ARRAYLIST VUOTA E TIMESTAMPSTART
                timestamp_start = timestamp_start +TimeUnit.HOURS.toMillis(1);
            }
        }else{
            Pair<String,Double> current_pair = new Pair<String,Double>(trip_id,distance);
            array_window.add(current_pair);
        }
        System.out.println("È ARRIVATO IL VIAGGIO "+trip_id+" APPERTENENTE ALLA FINESTRA CON TIME INIZIALE "+ windowStart+ " CON DISTANZA "+distance);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
