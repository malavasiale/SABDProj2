package query1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import preProcessing.ModifyCSVBolt;
import preProcessing.ReaderCSVSpout;

public class Query1Topology {

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source",new ReaderCSVSpout(),1);

        builder.setBolt("sector",new SectorConverterBolt().withTimestampField("field8").withTumblingWindow(BaseWindowedBolt.Duration.days(1)),1)
                .shuffleGrouping("source");

        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("query1", conf, builder.createTopology());
    }
}
