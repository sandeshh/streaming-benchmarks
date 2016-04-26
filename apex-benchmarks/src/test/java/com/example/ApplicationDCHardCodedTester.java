package com.example;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

public class ApplicationDCHardCodedTester extends ApplicationDCHardCoded
{
//  private static final Logger logger = LoggerFactory.getLogger(ApplicationDCHardCodedTester.class);

  protected long runTime = 6000000;

  @Before
  public void setUp()
  {
  }

  @Test
  public void test() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(PROP_STORE_PATH, "target/temp");

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(runTime);

    lc.shutdown();
  }

  @Override
  protected PubSubWebSocketAppDataResult createQueryResult(DAG dag, Configuration conf, AppDataSingleSchemaDimensionStoreHDHT store)
  {
    PubSubWebSocketAppDataResult wsResult = super.createQueryResult(dag, conf, store);
    wsResult.setTopic("resultTopic");
    return wsResult;
  }
  
  protected String getQueryUriString(DAG dag, Configuration conf)
  {
    return "ws://localhost:9090/pubsub";
  }
}
