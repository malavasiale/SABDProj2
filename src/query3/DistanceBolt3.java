package query3;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.javatuples.Quintet;
import utils.CalculateDistance;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DistanceBolt3 extends BaseWindowedBolt {
    String aa ="timestamp ship_id ship_type speed lon lat course heading date depart_port draught trip_id";
    private SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm");
    private SimpleDateFormat trip_id_format = new SimpleDateFormat("dd-MM-yy HH:mm");
    Date date_start;
    private long timestamp_start;
    private long timestamp_final;
    OutputCollector outputCollector;
    /*
    HashMap:
        -key : Trip_id
        -Value: Lat,Lon, Timestamp finestra, distanza percorsa, timestamp fine viaggio

     */
    Map<String, Quintet<Double,Double,Long,Double,Long>> active_trip = new HashMap<String,Quintet<Double,Double,Long,Double,Long>>();

    public DistanceBolt3() throws ParseException {
        date_start = format.parse("15-03-10 12:00");
        timestamp_start = date_start.getTime();
        timestamp_final = timestamp_start + TimeUnit.HOURS.toMillis(1);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {

        for( Tuple tuple : tupleWindow.get()){
            long timestamp = tuple.getLong(0);
            Double lon = Double.parseDouble(tuple.getString(4));
            Double lat = Double.parseDouble(tuple.getString(5));
            String date = tuple.getString(8);
            String trip_id = tuple.getString(11);
            String[] trip_id_split = trip_id.split(" - ");
            String trip_id_row = trip_id_split[1];
            String trip_id_end = trip_id_row.replaceAll("_parking","");
            long timestamp_trip_id_end = 0;
            try {
                Date trip_id_date = trip_id_format.parse(trip_id_end);
                timestamp_trip_id_end = trip_id_date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if(timestamp >= timestamp_final){
                while (timestamp >= timestamp_final){
                    timestamp_start = timestamp_final;
                    timestamp_final = timestamp_final + TimeUnit.HOURS.toMillis(1);
                }

                for(String trip_id_current : new ArrayList<String>(active_trip.keySet()) ){
                    Long windowTimestamp = active_trip.get(trip_id_current).getValue2();
                    Double current_dist = active_trip.get(trip_id_current).getValue3();
                    Long current_trip_id_end = active_trip.get(trip_id_current).getValue4();

                    outputCollector.emit(new Values(windowTimestamp,trip_id_current,current_dist));
                    if(current_trip_id_end<=timestamp_start){
                        active_trip.remove(trip_id_current);
                    }else{

                        Quintet<Double,Double,Long,Double,Long> old_quintet = active_trip.get(trip_id_current);
                        Double old_lat = old_quintet.getValue0();
                        Double old_lon = old_quintet.getValue1();
                        Double old_distance = old_quintet.getValue3();
                        Long old_timest_fv = old_quintet.getValue4();
                        active_trip.put(trip_id_current,new Quintet<Double,Double,Long,Double,Long>(old_lat,old_lon,timestamp_start,old_distance,old_timest_fv));
                    }

                }
            }
            if(active_trip.get(trip_id) != null){
                Quintet<Double,Double,Long,Double,Long> old_quintet = active_trip.get(trip_id);
                Double distance_to_add =CalculateDistance.euclideanDistance(old_quintet.getValue0(),old_quintet.getValue1(),lat,lon);
                Double old_distance = old_quintet.getValue3();
                Quintet<Double,Double,Long,Double,Long> new_quintet = new Quintet<Double,Double,Long,Double,Long>(lat,lon,timestamp_start,old_distance+distance_to_add,timestamp_trip_id_end);
                active_trip.put(trip_id,new_quintet);
                //System.out.println("AGGIRNATO VIAGGIO"+trip_id+" vECCHIA DISTANZA "+old_distance+"distanza aggiunta "+distance_to_add+"\n");
            }else{
                Quintet<Double,Double,Long,Double,Long> current_quintet = new Quintet<Double,Double,Long,Double,Long>(lat,lon,timestamp_start,0.0,timestamp_trip_id_end);
                active_trip.put(trip_id,current_quintet);
                //System.out.println("AGGIUNTO VIAGGIO "+trip_id+" QUINTET "+current_quintet.toString()+"\n");
            }


        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp","trip_id","distance"));
    }
}
