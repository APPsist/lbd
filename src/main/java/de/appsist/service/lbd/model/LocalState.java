package de.appsist.service.lbd.model;

public class LocalState
{
    private String state;
    private String station;
    private String type;
    private String priority;

    public String getState()
    {
        return state;
    }

    public void setState(String state)
    {
        this.state = state;
    }

    public String getStation()
    {
        return station;
    }

    public void setStation(String station)
    {
        this.station = station;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getPriority()
    {
        return priority;
    }

    public void setPriority(String priority)
    {
        this.priority = priority;
    }

    public LocalState()
    {

    }

    public LocalState(String state, String station, String type, String priority)
    {
        this.state = state;
        this.station = station;
        this.type = type;
        this.priority = priority;
    }

}
