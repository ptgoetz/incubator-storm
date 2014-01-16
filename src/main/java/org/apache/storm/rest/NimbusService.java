package org.apache.storm.rest;


import backtype.storm.Config;
import backtype.storm.utils.NimbusClient;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import org.apache.storm.rest.resources.NimbusResource;

import java.util.HashMap;
import java.util.Map;

public class NimbusService extends Service<NimbusServiceConfiguration> {

    public static void main(String[] args) throws Exception {
        new NimbusService().run(args);
    }

    @Override
    public void initialize(Bootstrap<NimbusServiceConfiguration> bootstrap) {
        bootstrap.setName("Storm Rest Service");
    }

    @Override
    public void run(NimbusServiceConfiguration config, Environment environment) throws Exception {

        ObjectMapperFactory factory = environment.getObjectMapperFactory();
        factory.enable(SerializationFeature.INDENT_OUTPUT);

        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.NIMBUS_HOST, config.getNimbusHost());
        conf.put(Config.NIMBUS_THRIFT_PORT, config.getNimbusPort());
        conf.put("storm.thrift.transport", "backtype.storm.security.auth.SimpleTransportPlugin");

        NimbusClient nc = NimbusClient.getConfiguredClient(conf);

        environment.addResource(new NimbusResource(nc.getClient()));
    }
}
