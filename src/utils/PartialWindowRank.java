package utils;

import org.javatuples.Pair;

import java.util.ArrayList;

public class PartialWindowRank {
    private ArrayList<Pair<String,Double>> partial_array = new ArrayList<Pair<String,Double>>();

    public PartialWindowRank(){

    }

    public boolean add(Pair<String,Double> to_add){
        return partial_array.add(to_add);
    }

    public void clear(){
        partial_array.clear();
    }

    public Pair<String,Double> get(Integer i){
        return partial_array.get(i);
    }

    public ArrayList<Pair<String,Double>> get_list(){
        return partial_array;
    }

    public Integer size(){
        return partial_array.size();
    }

}
