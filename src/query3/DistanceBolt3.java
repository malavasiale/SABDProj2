package query3;


import org.apache.storm.topology.base.BaseWindowedBolt;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.javatuples.Quintet;
import utils.CalculateDistance;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
    /*
    HashMap:
        -key : Trip_id
        -Value: Lat,Lon, Timestamp finestra, distanza percorsa, timestamp fine viaggio

     */
    Map<String, Quintet<Double,Double,Long,Double,Long>> active_trip = new HashMap<String,Quintet<Double,Double,Long,Double,Long>>();

    public DistanceBolt3() throws ParseException {
        date_start = format.parse("15-03-15 00:00");
        timestamp_start = date_start.getTime();
        timestamp_final = timestamp_start + TimeUnit.HOURS.toMillis(1);
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
                //System.out.println("QUI L HASHMAP DI DEVE SVUOTARE\n\n");
                //fare emit ed eliminare i viaggi conclusi
            }
            if(active_trip.get(trip_id) != null){
                Quintet<Double,Double,Long,Double,Long> old_quintet = active_trip.get(trip_id);
                Double distance_to_add =CalculateDistance.euclideanDistance(old_quintet.getValue0(),old_quintet.getValue1(),lat,lon);
                Double old_distance = old_quintet.getValue3();
                Quintet<Double,Double,Long,Double,Long> new_quintet = new Quintet<Double,Double,Long,Double,Long>(lat,lon,timestamp_start,old_distance+distance_to_add,timestamp_trip_id_end);
                active_trip.put(trip_id,new_quintet);
                System.out.println("AGGIRNATO VIAGGIO"+trip_id+" vECCHIA DISTANZA "+old_distance+"distanza aggiunta "+distance_to_add+"\n");
            }else{
                Quintet<Double,Double,Long,Double,Long> current_quintet = new Quintet<Double,Double,Long,Double,Long>(lat,lon,timestamp_start,0.0,timestamp_trip_id_end);
                active_trip.put(trip_id,current_quintet);
                System.out.println("AGGIUNTO VIAGGIO "+trip_id+" QUINTET "+current_quintet.toString()+"\n");
            }


        }

    }
}