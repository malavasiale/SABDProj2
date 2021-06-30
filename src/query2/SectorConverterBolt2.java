package query2;

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

import java.util.Map;

/**
 * Classe per convertire (lat,lon) -> sector_id
 */
public class SectorConverterBolt2 extends BaseWindowedBolt {
    OutputCollector collector;
    long start;
    AssignableMetric latency;


    @Override
    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
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
            String slat = t.getString(4);
            String slon = t.getString(3);
            //Conversione lato,lon in sector_id
            String id = ConvertToSector.convertPointToSector(Double.parseDouble(slat),Double.parseDouble(slon));

            String fascia_oraria = ConvertToSector.convertOrarioToFascia(t.getString(5));
            String sea;
            if(id.length()<5) { // perchè devo eliminare i dati non validi
                //Verificare se il settore appartiene al mare occidentale o orientale
                if (ConvertToSector.isOccidental(id)) {
                    sea = "occidentale";
                } else {
                    sea = "orientale";
                }
                long end = System.nanoTime();
                latency.setValue(new Long(end-start));
                start = 0;
                collector.emit(new Values(t.getLong(0),t.getString(1),fascia_oraria,t.getString(5),id,sea));
            }

        }

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp","ship_id","fascia","data","id_sector","sea"));
    }
}
