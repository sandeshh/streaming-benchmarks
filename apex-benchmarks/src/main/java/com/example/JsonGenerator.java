/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package com.example;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.*;

public class JsonGenerator extends BaseOperator implements InputOperator {

    public final transient DefaultOutputPort<JSONObject> out = new DefaultOutputPort<JSONObject>();

    private int adsIdx = 0;
    private int eventsIdx = 0;

    private String pageID = UUID.randomUUID().toString();
    private String userID = UUID.randomUUID().toString();
    private final String[] eventTypes = new String[]{"view", "click", "purchase"};

    private List<String> ads;
    private final Map<String, List<String>> campaigns;

    public JsonGenerator() {
        this.campaigns = generateCampaigns();
        this.ads = flattenCampaigns();
    }

    public Map<String, List<String>> getCampaigns() {
        return campaigns;
    }

    /**
     * Generate a single element
     */
    public JSONObject generateElement()
    {
        JSONObject jsonObject = new JSONObject();
        try       {

        jsonObject.put("user_id", userID);
        jsonObject.put("page_id", pageID);

        if (adsIdx == ads.size()) {
            adsIdx = 0;
        }
        if (eventsIdx == eventTypes.length) {
            eventsIdx = 0;
        }

        jsonObject.put("ad_id", ads.get(adsIdx++));
        jsonObject.put("ad_type", "banner78");
        jsonObject.put("event_type", eventTypes[eventsIdx++]);
        jsonObject.put("event_time", System.currentTimeMillis());
        jsonObject.put("ip_address", "1.2.3.4");
    }
    catch ( JSONException json){

    }

        return jsonObject;
    }

    /**
     * Generate a random list of ads and campaigns
     */
    private Map<String, List<String>> generateCampaigns() {
        int numCampaigns = 100;
        int numAdsPerCampaign = 10;
        Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
        for (int i = 0; i < numCampaigns; i++) {
            String campaign = UUID.randomUUID().toString();
            ArrayList<String> ads = new ArrayList<>();
            adsByCampaign.put(campaign, ads);
            for (int j = 0; j < numAdsPerCampaign; j++) {
                ads.add(UUID.randomUUID().toString());
            }
        }
        return adsByCampaign;
    }

    /**
     * Flatten into just ads
     */
    private List<String> flattenCampaigns() {
        // Flatten campaigns into simple list of ads
        List<String> ads = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
            for (String ad : entry.getValue()) {
                ads.add(ad);
            }
        }
        return ads;
    }

    @Override
    public void emitTuples() {
        out.emit( generateElement() ) ;
    }
}
