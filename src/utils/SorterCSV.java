package utils;

import com.opencsv.*;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class SorterCSV {

    public static void main(String[] args) throws IOException, ParseException {
        FileReader filereader;
        CSVReader csvReader=null;
        List<String[]> to_sort = new ArrayList<String[]>();

        try {
            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
            filereader = new FileReader("../prj2_dataset.csv");
            csvReader = new CSVReaderBuilder(filereader)
                    .withCSVParser(parser)
                    .withSkipLines(1)
                    .build();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        SimpleDateFormat input_format1=new SimpleDateFormat("dd-MM-yy HH:mm");
        SimpleDateFormat input_format2=new SimpleDateFormat("dd/MM/yy HH:mm");

        SimpleDateFormat format2 = new SimpleDateFormat("yy-MM-dd HH:mm");
        Date date;

        String[] row;
        while((row = csvReader.readNext()) != null){
            if(row[4].contains("-")){
                date = input_format1.parse(row[4]);
            }else{
                date = input_format2.parse(row[4]);
            }
            row[4] = format2.format(date);
            to_sort.add(row);
        }

        to_sort.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] l1, String[] l2) {
                return l1[4].compareTo(l2[4]);
            }
        });

        CSVWriter writer = new CSVWriter(new FileWriter("../dataset_sorted.csv",true),CSVWriter.DEFAULT_SEPARATOR,CSVWriter.NO_QUOTE_CHARACTER);
        writer.writeAll(to_sort);
        writer.close();
    }

}
