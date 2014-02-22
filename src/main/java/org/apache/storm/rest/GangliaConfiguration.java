package org.apache.storm.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;

public class GangliaConfiguration extends Configuration {
    @NotEmpty
    @JsonProperty
    private String host = "localhost";

    @Min(1)
    @Max(65535)
    @JsonProperty
    private int port = 8649;

    @Pattern(regexp = "UNICAST|MULTICAST")
    @JsonProperty
    private String addressMode = "MULTICAST";

    @JsonProperty
    private String spoof;

    @Min(10)
    @JsonProperty
    private int reportInterval = 60;


    public String getAddressMode(){
        return addressMode;
    }

    public boolean isUnicast(){
        return this.addressMode.equals("UNICAST");
    }

    public int getReportInterval(){
        return this.reportInterval;
    }

    public String getHost(){
        return this.host;
    }

    public String getSpoof(){
        return this.spoof;
    }

    public int getPort(){
        return this.port;
    }

}
