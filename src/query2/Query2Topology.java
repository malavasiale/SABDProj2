package query2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import query1.*;

public class Query2Topology {

    public static void main(String[] args){

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source",new ReaderCSVSpout2(),1);

        builder.setBolt("sector",new SectorConverterBolt2().withTimestampExtractor(new TimestampExtractor() {
            @Override
            public long extractTimestamp(Tuple tuple) {
                return tuple.getLong(0);
            }
        }).withTumblingWindow((BaseWindowedBolt.Duration.minutes(30))),1)
                .shuffleGrouping("source");

        builder.setBolt("count",new CountBolt2(),1)
                .shuffleGrouping("sector");

        builder.setBolt("sum",new SumBolt2(args[0]),3)
                .fieldsGrouping("count",new Fields("sector_id"));
        builder.setBolt("rank", new RankBolt2(),2)
                .fieldsGrouping("sum", new Fields("sea"));

        builder.setBolt("exporter",
                new RabbitMQExporterBolt2(
                        "rabbitmq","rabbitmq" ,
                        "rabbitmq", "query2"),
                3)
                .shuffleGrouping("rank");


        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,false);
        conf.put(Config.TOPOLOGY_DEBUG,false);
        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("query2", conf, builder.createTopology());
    }
}
