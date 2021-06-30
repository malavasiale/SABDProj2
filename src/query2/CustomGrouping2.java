package query2;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class CustomGrouping2 implements CustomStreamGrouping {

    private List<Integer> task_1 = new ArrayList<>();
    private List<Integer> task_2 = new ArrayList<>();
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        task_1.add(list.get(0));
        task_2.add(list.get(1));
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {

        String current_sea = list.get(3).toString();
        if(current_sea.equals("occidentale")){
            return task_1;
        }else{
            return task_2;
        }
    }
}
