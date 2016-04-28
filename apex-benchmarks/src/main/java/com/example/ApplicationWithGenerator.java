package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

/**
 * Created by sandesh on 3/18/16.
 */
@ApplicationAnnotation(name = "ApplicationWithGenerator")
public class ApplicationWithGenerator implements StreamingApplication {
    @Override
    public void populateDAG(DAG dag, Configuration configuration) {

        // Create operators for each step
        // settings are applied by the platform using the config file.

        EventGenerator2 eventGenerator = dag.addOperator("eventGenerator", new EventGenerator2());
        NullOperator nulllOperator = dag.addOperator("nullOperator", new NullOperator());

        DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
        FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples() );
        FilterFields2 filterFields = dag.addOperator("filterFields", new FilterFields2() );
        RedisJoin2 redisJoin = dag.addOperator("redisJoin", new RedisJoin2());

        /*
        CampaignProcessor2 campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessor2());
*/
        setupRedis(eventGenerator.getCampaigns());

        // Connect the Ports in the Operators
        dag.addStream("deserialize", eventGenerator.out, deserializeJSON.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("filterTuples", deserializeJSON.output, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

        // dag.addStream("output", redisJoin.output, campaignProcessor.input);
        dag.addStream("output", redisJoin.output, nulllOperator.input);

        dag.setInputPortAttribute(deserializeJSON.input, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
      //  dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);

        dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(1));


       // dag.addStream("deserialize", eventGenerator.out, nulllOperator.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    }

    private void setupRedis(Map<String, List<String>> campaigns) {

        RedisHelper redisHelper = new RedisHelper();
        redisHelper.init("node35.morado.com");

        redisHelper.prepareRedis2(campaigns);
    }
}
