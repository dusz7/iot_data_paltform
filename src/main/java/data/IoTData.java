package data;

import org.json.JSONObject;

import java.io.Serializable;
import java.time.Instant;

public class IoTData implements Serializable {
    private Instant dataTime;
    private String dataId;

    private String dataType;
    private JSONObject dataValue;

    public IoTData(Instant dataTime, String dataId, String dataType, JSONObject dataValue) {
        this.dataTime = dataTime;
        this.dataId = dataId;
        this.dataType = dataType;
        this.dataValue = dataValue;
    }

    public Instant getDataTime() {
        return dataTime;
    }

    public void setDataTime(Instant dataTime) {
        this.dataTime = dataTime;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public JSONObject getDataValue() {
        return dataValue;
    }

    public void setDataValue(JSONObject dataValue) {
        this.dataValue = dataValue;
    }

    @Override
    public String toString() {
        return "IoTData [dataTime=" + dataTime.toString() + ", dataId=" + dataId + ", dataType=" + dataType + ", dataValue="
                + dataValue.toString()+ "]";
    }
}
