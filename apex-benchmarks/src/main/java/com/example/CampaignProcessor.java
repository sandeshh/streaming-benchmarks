package com.example;

import benchmark.common.advertising.CampaignProcessorCommon;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by sandesh on 3/18/16.
 */
public class CampaignProcessor extends BaseOperator
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
                campaignProcessorCommon.execute(String.valueOf(tuple.campaignId), String.valueOf(tuple.event_time));
            }
            catch ( Exception exception ) {
                throw new RuntimeException( "" + tuple.campaignId + ", " + Long.toString(tuple.event_time) );
            }
        }
    };

    public void setup(Context.OperatorContext context)
    {
        campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
        this.campaignProcessorCommon.prepare();
    }
}
