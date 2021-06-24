package query3;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import query2.*;

import java.text.ParseException;

public class Query3Topology {

    public static void main(String[] args) throws ParseException {
        //TODO Aumentare Parallelismo su Sum & Rank
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source",new ReaderCSVSpout3(),1);


        builder.setBolt("distance",new DistanceBolt3(args[0]).withTimestampExtractor(new TimestampExtractor() {
            @Override
            public long extractTimestamp(Tuple tuple) {
                return tuple.getLong(0);
            }
        }).withTumblingWindow((BaseWindowedBolt.Duration.minutes(30))),1)
                .shuffleGrouping("source");

        builder.setBolt("partial",new PartialRanckBolt3(args[0]),2)
                .fieldsGrouping("distance",new Fields("trip_id"));

        builder.setBolt("global",new GlobalRank3(),1)
                .shuffleGrouping("partial");


        builder.setBolt("exporter",
                new RabbitMQExporterBolt3(
                        "rabbitmq","rabbitmq" ,
                        "rabbitmq", "query3"),
                3)
                .shuffleGrouping("global");

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,false);
        conf.put(Config.TOPOLOGY_DEBUG,false);
        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("query3", conf, builder.createTopology());
    }
}
