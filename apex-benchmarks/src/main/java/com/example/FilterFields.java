package com.example;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

@Stateless
public class FilterFields extends BaseOperator
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
