/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package com.example;

import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.RedisAdCampaignCache;
import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.netlet.util.DTThrowable;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationAnnotation(name = "Apex_Benchmark")
public class Application implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create operators for each step
    // settings are applied by the platform using the config file.
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput", new KafkaSinglePortStringInputOperator());
    DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples() );
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields() );
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
    CampaignProcessor campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessor());

    // kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    // Connect the Ports in the Operators
    dag.addStream("deserialize", kafkaInput.outputPort, deserializeJSON.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterTuples", deserializeJSON.output, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("redisJoin", filterFields.output, redisJoin.input);
    dag.addStream("output", redisJoin.output, campaignProcessor.input);

    dag.setInputPortAttribute(deserializeJSON.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
  }

  @Stateless
  public static class DeserializeJSON extends BaseOperator
  {
    public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override
      public void process(String t)
      {
        JSONObject jsonObject;
        try {
          jsonObject = new JSONObject(t);
        } catch (JSONException e) {
          throw DTThrowable.wrapIfChecked(e);
        }

        output.emit(jsonObject);
      }
    };

    public transient DefaultOutputPort<JSONObject> output = new DefaultOutputPort();
  }

  @Stateless
  public static class FilterFields extends BaseOperator
  {
    public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
    {
      @Override
      public void process(JSONObject jsonObject)
      {
        try {

          Tuple tuple = new Tuple();

          tuple.ad_id = jsonObject.getString("ad_id");
          tuple.event_ime = jsonObject.getString("event_time");

          if ( tuple.event_ime == null ) return ;

          output.emit(tuple);
        } catch (JSONException e) {
          DTThrowable.wrapIfChecked(e);
        }
      }
    };

    public transient DefaultOutputPort<Tuple> output = new DefaultOutputPort();
  }

  @Stateless
  public static class FilterTuples extends BaseOperator
  {
    private static final Logger LOG = LoggerFactory.getLogger(FilterTuples.class);

    public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
    {
      @Override
      public void process(JSONObject jsonObject)
      {
        try {
          LOG.info(" Message : {} ", input.toString());
          if (  jsonObject.getString("event_type").equals("view") ) {
            output.emit(jsonObject);
          }
        } catch (JSONException e) {
          DTThrowable.wrapIfChecked(e);
        }
      }
    };

    public transient DefaultOutputPort<JSONObject> output = new DefaultOutputPort();
  }

  public static class RedisJoin extends BaseOperator
  {
    private transient RedisAdCampaignCache redisAdCampaignCache;
    private String redisServerHost;

    public String getRedisServerHost()
    {
      return redisServerHost;
    }

    public void setRedisServerHost(String redisServerHost)
    {
      this.redisServerHost = redisServerHost;
    }

    public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
    {
      @Override
      public void process(Tuple tuple)
      {
        String campaign_id = redisAdCampaignCache.execute(tuple.ad_id);

        if (campaign_id == null || campaign_id.isEmpty()) {
          return;
        }

        tuple.campaign_id = campaign_id ;

        output.emit(tuple);
      }
    };

    public transient DefaultOutputPort<Tuple> output = new DefaultOutputPort();

    @Override
    public void setup(Context.OperatorContext context)
    {
      this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
      this.redisAdCampaignCache.prepare();
    }
  }

  public static class CampaignProcessor extends BaseOperator
  {
    private transient CampaignProcessorCommon campaignProcessorCommon;
    private String redisServerHost;

    public String getRedisServerHost()
    {
      return redisServerHost;
    }

    public void setRedisServerHost(String redisServerHost)
    {
      this.redisServerHost = redisServerHost;
    }

    public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
    {
      @Override
      public void process(Tuple tuple)
      {
        try {
          campaignProcessorCommon.execute(tuple.campaign_id, tuple.event_ime);
        }
        catch ( Exception exception ) {
           throw new RuntimeException( tuple.campaign_id + tuple.event_ime );
        }
      }
    };

    public void setup(Context.OperatorContext context)
    {
      campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
      this.campaignProcessorCommon.prepare();
    }
  }

  public static class Tuple {
    public String ad_id ;
    public String campaign_id;
    public String event_ime ;
  }

}
