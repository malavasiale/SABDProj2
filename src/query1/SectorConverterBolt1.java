package query1;

import org.apache.storm.metric.api.AssignableMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import utils.ConvertToSector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class SectorConverterBolt1 extends BaseWindowedBolt {
    OutputCollector collector;
    private DateFormat dateformat;
    long start;
    AssignableMetric latency;


    @Override
    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.dateformat = new SimpleDateFormat("yy-MM-dd");
        latency = new AssignableMetric(new Long(0));
        start= 0;
        context.registerMetric("Latency-sector",latency,10);

    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        if(start == 0){
            start = System.nanoTime();
        }
        for(Tuple t : tupleWindow.get()){
            //Vengono prese la latitudine e la longitudine della tupla
            String slat = t.getString(4);
            String slon = t.getString(3);
            //Conversione (lat,lon) --> Secot_id
            String id = ConvertToSector.convertPointToSector(Double.parseDouble(slat),Double.parseDouble(slon));
            //Controllo per evitare di passere dati non validi
            if(id.length()<5){
                //Verifica se un settore appartiene o no al Mar Mediterraneo Occidentale
                if(ConvertToSector.isOccidental(id)){

                    String ship_type = ConvertToSector.shipType(t.getString(2));
                    Date date = new Date(t.getLong(0));
                    String string_date = dateformat.format(date);
                    //Emit al Bolt successivo
                    long end = System.nanoTime();
                    latency.setValue(new Long(end-start));
                    start = 0;
                    collector.emit(new Values(t.getLong(0),t.getString(1),ship_type,t.getString(5),id,string_date));
                }
            }

        }

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp","ship_id","ship_type","data","id_sector","day"));
    }
}
