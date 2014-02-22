package org.apache.storm.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;


public class NimbusServiceConfiguration extends Configuration {

    @NotEmpty
    @JsonProperty
    private String nimbusHost;

    @Min(1)
    @Max(65535)
    @JsonProperty
    private int nimbusPort;

    @JsonProperty
    private boolean enableGanglia = false;

    @Valid
    @JsonProperty
    private GangliaConfiguration ganglia;

    public NimbusServiceConfiguration(){}

    public int getNimbusPort(){
        return this.nimbusPort;
    }

    public String getNimbusHost(){
        return this.nimbusHost;
    }

    public boolean isEnableGanglia(){
        return this.enableGanglia;
    }

    public GangliaConfiguration getGanglia(){
        return this.ganglia;
    }
}
