package query1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

public class Query1Topology {

    public static void main(String[] args){
        //TODO AGGIUNGERE IN COUNTBOLT FIELD "SECTOR_ID" AGGIUNGERE LA PARTE CHE INSERISCE I GIORNI MANCANTI E MODIFICIARE
        // IL NUMERO DI TUPLE ATTESE IN SUM = VALORE CORRENTE*NUMERO REPLICHE COUNTBOLT
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source",new ReaderCSVSpout1(),1);

        builder.setBolt("sector",new SectorConverterBolt1().withTimestampExtractor(new TimestampExtractor() {
            @Override
            public long extractTimestamp(Tuple tuple) {
                return tuple.getLong(0);
            }
        }).withTumblingWindow((BaseWindowedBolt.Duration.minutes(30))),1)
                .shuffleGrouping("source");

        builder.setBolt("count",new CountBolt1(),3)
                .fieldsGrouping("sector",new Fields("day"));

        builder.setBolt("sum",new SumBolt1(args[0]),3)
                .fieldsGrouping("count", new Fields("sector_id"));

        builder.setBolt("exporter",
                new RabbitMQExporterBolt1(
                        "rabbitmq","rabbitmq" ,
                        "rabbitmq", "query1"),
                3)
                .shuffleGrouping("sum");

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,false);
        conf.put(Config.TOPOLOGY_DEBUG,false);
        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("query1", conf, builder.createTopology());
    }
}
