package com.example;

import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.CampaignWindowPair;
import benchmark.common.advertising.LRUHashMap;
import benchmark.common.advertising.Window;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by sandesh on 4/6/16.
 *
 * TODO : Extension : Use the streamcodec to send the tuples to particular partition, so that there will be only one writer for one time window.
 */
public class CampaignProcessorWithApexWindow extends BaseOperator {

    private static final Logger LOG = LoggerFactory.getLogger(CampaignProcessorCommon.class);
    private transient Jedis jedis;
    private transient Jedis flush_jedis;
    private Long lastWindowMillis;
    // Bucket -> Campaign_id -> Window
    private LRUHashMap<Long, HashMap<String, Window>> campaign_windows;

    private Set<CampaignWindowPair> need_flush;

    private long processed = 0;

    private static final Long time_divisor = 10000L; // 10 second windows
    private String redisServerHost = "node35";

    public String getRedisServerHost()
    {
        return redisServerHost;
    }

    public void setRedisServerHost(String redisServerHost)
    {
        this.redisServerHost = redisServerHost;
    }

    public CampaignProcessorWithApexWindow() {
        jedis = new Jedis(redisServerHost);
        flush_jedis = new Jedis(redisServerHost);
    }

    @Override
    public void endWindow(){

        flushWindows();
        lastWindowMillis = System.currentTimeMillis();
    }

    public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
    {
        @Override
        public void process(Tuple tuple)
        {
            try {

                Long timeBucket = Long.parseLong(tuple.event_ime) / time_divisor;
                Window window = getWindow(timeBucket, tuple.campaign_id);
                window.seenCount++;

                CampaignWindowPair newPair = new CampaignWindowPair(tuple.campaign_id, window);
                need_flush.add(newPair);

                processed++;
            }
            catch ( Exception exception ) {
                throw new RuntimeException( tuple.campaign_id + tuple.event_ime );
            }
        }
    };

    public void setup(Context.OperatorContext context)
    {
        campaign_windows = new LRUHashMap<Long, HashMap<String, Window>>(10);
        lastWindowMillis = System.currentTimeMillis();
        need_flush = new HashSet<CampaignWindowPair>();
    }

    private void flushWindows() {
        for (CampaignWindowPair pair : need_flush) {
            writeWindow(pair.campaign, pair.window);
        }
        need_flush.clear();
    }

    private static Window redisGetWindow(Long timeBucket, Long time_divisor) {

        Window win = new Window();
        win.timestamp = Long.toString(timeBucket * time_divisor);
        win.seenCount = 0L;
        return win;
    }

    // Needs to be rewritten now that redisGetWindow has been simplified.
    // This can be greatly simplified.
    private Window getWindow(Long timeBucket, String campaign_id) {
        synchronized (campaign_windows) {
            HashMap<String, Window> bucket_map = campaign_windows.get(timeBucket);
            if (bucket_map == null) {
                // Try to pull from redis into cache.
                Window redisWindow = redisGetWindow(timeBucket, time_divisor);
                if (redisWindow != null) {
                    bucket_map = new HashMap<String, Window>();
                    campaign_windows.put(timeBucket, bucket_map);
                    bucket_map.put(campaign_id, redisWindow);
                    return redisWindow;
                }

                // Otherwise, if nothing in redis:
                bucket_map = new HashMap<String, Window>();
                campaign_windows.put(timeBucket, bucket_map);
            }

            // Bucket exists. Check the window.
            Window window = bucket_map.get(campaign_id);
            if (window == null) {
                // Try to pull from redis into cache.
                Window redisWindow = redisGetWindow(timeBucket, time_divisor);
                if (redisWindow != null) {
                    bucket_map.put(campaign_id, redisWindow);
                    return redisWindow;
                }

                // Otherwise, if nothing in redis:
                window = new Window();
                window.timestamp = Long.toString(timeBucket * time_divisor);
                window.seenCount = 0L;
                bucket_map.put(campaign_id, redisWindow);
            }
            return window;
        }
    }

    private void writeWindow(String campaign, Window win) {
        String windowUUID = flush_jedis.hmget(campaign, win.timestamp).get(0);
        if (windowUUID == null) {
            windowUUID = UUID.randomUUID().toString();
            flush_jedis.hset(campaign, win.timestamp, windowUUID);

            String windowListUUID = flush_jedis.hmget(campaign, "windows").get(0);
            if (windowListUUID == null) {
                windowListUUID = UUID.randomUUID().toString();
                flush_jedis.hset(campaign, "windows", windowListUUID);
            }
            flush_jedis.lpush(windowListUUID, win.timestamp);
        }

        flush_jedis.hincrBy(windowUUID, "seen_count", win.seenCount);
        win.seenCount = 0L;

        flush_jedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
        flush_jedis.lpush("time_updated", Long.toString(System.currentTimeMillis()));
    }
}
