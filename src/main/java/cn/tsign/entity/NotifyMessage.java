package cn.tsign.entity;

import jodd.util.StringUtil;

import java.util.ArrayList;
import java.util.HashSet;

public class NotifyMessage {

    private String executedQuery;

    private String operationName;

    private ArrayList<String> inputs;

    private ArrayList<String> outputs;

    private HashSet<String> tables;

    public void addTables(String database,String table){
        if(tables == null ){
            tables = new HashSet<>();
        }
        if(StringUtil.isEmpty(database)||StringUtil.isEmpty(table)){
            return ;
        }
        tables.add(database+"."+table);
    }

    public HashSet<String> getTables() {
        return tables;
    }

    public void setTables(HashSet<String> tables) {
        this.tables = tables;
    }

    public String getExecutedQuery() {
        return executedQuery;
    }

    public void setExecutedQuery(String executedQuery) {
        this.executedQuery = executedQuery;
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public ArrayList<String> getInputs() {
        return inputs;
    }

    public void setInputs(ArrayList<String> inputs) {
        this.inputs = inputs;
    }

    public void addInput(String input) {
        if(this.inputs == null )this.inputs = new ArrayList<>();
        this.inputs.add(input);
    }

    public ArrayList<String> getOutputs() {
        return outputs;
    }

    public void setOutputs(ArrayList<String> outputs) {
        this.outputs = outputs;
    }

    public void addOutput(String output) {
        if(this.outputs == null )this.outputs = new ArrayList<>();
        this.outputs.add(output);
    }
}
