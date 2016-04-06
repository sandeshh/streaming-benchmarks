package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by sandesh on 3/18/16.
 */
@ApplicationAnnotation(name = "ApplicationWithGenerator")
public class ApplicationWithGenerator implements StreamingApplication {
    @Override
    public void populateDAG(DAG dag, Configuration configuration) {

        // Create operators for each step
        // settings are applied by the platform using the config file.

        EventGenerator eventGenerator = dag.addOperator("eventGenerator", new EventGenerator());
        DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
        FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples() );
        FilterFields filterFields = dag.addOperator("filterFields", new FilterFields() );
        RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
        CampaignProcessorWithApexWindow campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessorWithApexWindow());

        // kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

        // Connect the Ports in the Operators
        dag.addStream("deserialize", eventGenerator.out, deserializeJSON.input) ;
        dag.addStream("filterTuples", deserializeJSON.output, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("output", redisJoin.output, campaignProcessor.input);

        dag.setInputPortAttribute(deserializeJSON.input, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);

        // dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(5));
    }
}
