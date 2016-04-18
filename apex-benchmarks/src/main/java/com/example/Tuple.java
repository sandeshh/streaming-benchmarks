package com.example;

/**
 * Created by sandesh on 3/18/16.
 */
public class Tuple {
    public String ad_id ;
    public String campaign_id;
    public Long event_ime ;
    public long clicks = 0;
    
    public Tuple(){}
    
    public Tuple(String adId, String campaignId, long eventTime, long clicks)
    {
      this.ad_id = adId;
      this.campaign_id = campaignId;
      this.event_ime = eventTime;
      this.clicks = clicks;
    }
    
    public String getAdId()
    {
      return ad_id;
    }
    public void setAdId(String adId)
    {
      this.ad_id = adId;
    }
    
    public String getCampaignId()
    {
      return campaign_id;
    }
    public void setCampaignId(String campaignId)
    {
      this.campaign_id = campaignId;
    }
    
    public long getEventTime()
    {
      return (event_ime == null) ? 0 : event_ime;
    }
    public void setEventTime(long eventTime)
    {
      this.event_ime = eventTime;
    }
    
    public long getTime()
    {
      return getEventTime();
    }
    
    public long getClicks()
    {
      return clicks;
    }
    public void setClicks(long clicks)
    {
      this.clicks = clicks;
    }
}
