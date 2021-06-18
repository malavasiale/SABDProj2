package query1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

public class Query1Topology {

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source",new ReaderCSVSpout(),1);

        builder.setBolt("sector",new SectorConverterBolt().withTimestampExtractor(new TimestampExtractor() {
            @Override
            public long extractTimestamp(Tuple tuple) {
                return tuple.getLong(0);
            }
        }).withTumblingWindow((BaseWindowedBolt.Duration.minutes(5))),3)
                .shuffleGrouping("source");

        Config conf = new Config();
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,400000);
        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("query1", conf, builder.createTopology());
    }
}
