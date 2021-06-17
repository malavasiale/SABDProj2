package utils;

import com.opencsv.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SorterCSV {

    public static void main(String[] args) throws IOException {

        List<String[]> to_sort = new ArrayList<String[]>();

        CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
        FileReader fileReader = new FileReader("../dataset.csv");
        CSVReader csvReader = new CSVReaderBuilder(fileReader)
                .withCSVParser(parser)
                .build();


        String[] row;
        while((row = csvReader.readNext()) != null){
            to_sort.add(row);
        }

        to_sort.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] l1, String[] l2) {
                return l1[7].compareTo(l2[7]);
            }
        });

        CSVWriter writer = new CSVWriter(new FileWriter("../dataset_sorted.csv",true));
        writer.writeAll(to_sort);
    }

}
