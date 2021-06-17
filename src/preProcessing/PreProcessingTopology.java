package preProcessing;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class PreProcessingTopology {

    public static void main(){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source",new ReaderCSVSpout(),1);

        builder.setBolt("modify",new ModifyCSVBolt(),2)
                .shuffleGrouping("source");

        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("preprocessing", conf, builder.createTopology());
    }

}
