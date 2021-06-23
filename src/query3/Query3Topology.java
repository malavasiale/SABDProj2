package query3;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import query2.*;

import java.text.ParseException;

public class Query3Topology {

    public static void main(String[] args) throws ParseException {
        //TODO Aumentare Parallelismo su Sum & Rank
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source",new ReaderCSVSpout3(),1);


        builder.setBolt("distance",new DistanceBolt3().withTimestampExtractor(new TimestampExtractor() {
            @Override
            public long extractTimestamp(Tuple tuple) {
                return tuple.getLong(0);
            }
        }).withTumblingWindow((BaseWindowedBolt.Duration.minutes(60))),1)
                .shuffleGrouping("source");

        builder.setBolt("partial",new PartialRanckBolt3(),1)
                .shuffleGrouping("distance");
        /**
        builder.setBolt("sum",new SumBolt2("month"),1)
                .shuffleGrouping("count");
        builder.setBolt("rank", new RankBolt2(),1)
                .shuffleGrouping("sum");
        /**
        builder.setBolt("exporter",
                new RabbitMQExporterBolt1(
                        "rabbitmq","rabbitmq" ,
                        "rabbitmq", "query1"),
                3)
                .shuffleGrouping("sum");
        **/
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,false);
        conf.put(Config.TOPOLOGY_DEBUG,false);
        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("query3", conf, builder.createTopology());
    }
}
