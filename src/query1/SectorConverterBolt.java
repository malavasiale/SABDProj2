package query1;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import utils.ConvertToSector;

public class SectorConverterBolt extends BaseWindowedBolt {



    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple t : tupleWindow.get()){
            String slat = t.getString(4);
            String slon = t.getString(3);

            String id = ConvertToSector.convertPointToSector(Double.parseDouble(slat),Double.parseDouble(slon));
            System.out.println("ID DEL SETTORE : "+id+"\n");
        }
    }
}
