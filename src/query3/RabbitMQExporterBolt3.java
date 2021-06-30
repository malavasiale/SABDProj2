package query3;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import utils.RabbitMQManager;

import java.util.Map;

public class RabbitMQExporterBolt3  extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private RabbitMQManager rabbitmq;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;
    private String defaultQueue;
    private CountMetric metric;


    public RabbitMQExporterBolt3(String rabbitMqHost, String rabbitMqUsername, String rabbitMqPassword,
                                 String defaultQueue) {
        super();
        this.rabbitMqHost = rabbitMqHost;
        this.rabbitMqUsername = rabbitMqUsername;
        this.rabbitMqPassword = rabbitMqPassword;
        this.defaultQueue = defaultQueue;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.metric = new CountMetric();
        topologyContext.registerMetric("Throughput",metric,15);
        this.collector=outputCollector;
        this.rabbitmq = new RabbitMQManager(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, defaultQueue);

    }

    @Override
    public void execute(Tuple tuple) {
        metric.incr();
        String output = tuple.getString(0);
        rabbitmq.send("query3",output);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


}
