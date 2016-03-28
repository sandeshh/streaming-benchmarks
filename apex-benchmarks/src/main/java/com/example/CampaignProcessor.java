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
