package preProcessing;

import com.opencsv.CSVWriter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.io.FileWriter;
import java.io.IOException;

public class ModifyCSVBolt extends BaseBasicBolt {

    CSVWriter writer;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            writer = new CSVWriter(new FileWriter("/data/dataset.csv",true),CSVWriter.DEFAULT_SEPARATOR,CSVWriter.NO_QUOTE_CHARACTER);
            String[] currentline = new String[11];
            currentline[0] = tuple.getString(0);
            currentline[1] = tuple.getString(1);
            currentline[2] = tuple.getString(2);
            currentline[3] = tuple.getString(3);
            currentline[4] = tuple.getString(4);
            currentline[5] = tuple.getString(5);
            currentline[6] = tuple.getString(6);
            String timestamp = tuple.getString(7);
            System.out.println("HO LETTO LA DATA : " + timestamp);
            SimpleDateFormat input_format1=new SimpleDateFormat("dd-MM-yy HH:mm");
            SimpleDateFormat input_format2=new SimpleDateFormat("dd/MM/yy HH:mm");

            SimpleDateFormat format2 = new SimpleDateFormat("yy-MM-dd HH:mm");
            Date date;
            if(timestamp.contains("-")){
                date = input_format1.parse(timestamp);
            }else{
                date = input_format2.parse(timestamp);
            }

            currentline[7] = format2.format(date);
            currentline[8] = tuple.getString(8);
            currentline[9] = tuple.getString(9);
            currentline[10] = tuple.getString(10);

            writer.writeNext(currentline);
            writer.flush();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
