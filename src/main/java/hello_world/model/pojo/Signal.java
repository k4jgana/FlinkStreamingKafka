package hello_world.model.pojo;

import org.json.JSONObject;


public class Signal {
    public JSONObject jsonObject;

    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String key;
    public int value;

    public long timestamp;



    public Signal(JSONObject jsonObject) {
        this.jsonObject = jsonObject;

    }

    public Signal(String jsonString) {

        this.jsonObject = new JSONObject(jsonString);

        this.key=this.getFieldValue("key");

        this.value=this.getFieldAsInteger("value");

        this.timestamp=Long.parseLong(this.getFieldValue("timestamp"));

    }

    public Signal(String key,int value,Long timestamp)
    {
        this.key=key;
        this.value=value;
        this.timestamp=timestamp;

    }

    public static Signal createSignal(String data)
    {
        JSONObject jsonObject = new JSONObject(data);
        return new Signal(jsonObject
                .get("key").toString()
                ,Integer.parseInt(jsonObject.get("value").toString())
                ,Long.parseLong(jsonObject.get("value").toString()));
    }






    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public boolean hasField(String field) {
        return jsonObject.has(field);
    }

    public String getFieldValue(String field) {
        if (hasField(field)) {
            return jsonObject.get(field).toString();
        } else {
            return null;
        }
    }

    public Integer getFieldAsInteger(String field){
        if (hasField(field)){
            return Integer.parseInt(jsonObject.get(field).toString());
        } else {
            return null;
        }
    }



    @Override
    public String toString() {
        return jsonObject.toString();
    }
}