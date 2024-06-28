package com.flipkart.fdp.superbi.cosmos.data.query.response;

import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;

/**
 * User: aniruddha.gangopadhyay
 * Date: 04/02/14
 * Time: 3:33 PM
 */
public class DataResponse {

    private class Input{
        public List<String> columns;
        public Map<String,Object> parameters;

        private Input(List<String> columns, Map<String, Object> parameters) {
            this.columns = columns;
            this.parameters = parameters;
        }
    }

    private class Output{
        public List<String> schema;
        public List<List<Object>> data;

        private Output(List<String> schema, List<ResultRow> data) {
            this.schema = schema;
            this.data = Lists.newArrayList();
            for(ResultRow resultRow : data)
                this.data.add(resultRow.row);
        }
    }

    public Input input;
    public Output output;

//    public DataResponse(Map<String, String> columns, List<FilterParam> filterParams, Map<String, Object> additionalParams, QueryResult result) {
//        Map<String,Object> parameters = Maps.newHashMap();
//        if(filterParams!=null){
//            for(FilterParam filterParam : filterParams){
//                parameters.put(filterParam.getFieldName(),filterParam);
//            }
//        }
//        if(additionalParams!=null)
//            parameters.putAll(additionalParams);
//        this.input = new Input(Lists.newArrayList(columns.keySet()),parameters);
//        this.output = new Output(result.schema,result.data);
//    }
}
