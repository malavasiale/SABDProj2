package query2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import utils.RabbitMQManager;

import java.io.FileWriter;
import java.util.Map;

public class RabbitMQExporterBolt2 extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private RabbitMQManager rabbitmq;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;
    private String defaultQueue;
    FileWriter myWriter;

    public RabbitMQExporterBolt2(String rabbitMqHost, String rabbitMqUsername, String rabbitMqPassword,
                                 String defaultQueue) {
        super();
        this.rabbitMqHost = rabbitMqHost;
        this.rabbitMqUsername = rabbitMqUsername;
        this.rabbitMqPassword = rabbitMqPassword;
        this.defaultQueue = defaultQueue;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        this.rabbitmq = new RabbitMQManager(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, defaultQueue);

    }

    @Override
    public void execute(Tuple tuple) {

        String output = tuple.getString(0);
        rabbitmq.send("query2",output);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }



}