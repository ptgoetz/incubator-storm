package org.apache.storm.rest;


import backtype.storm.Config;
import backtype.storm.utils.NimbusClient;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import org.apache.storm.rest.resources.NimbusResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class NimbusService extends Service<NimbusServiceConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusService.class);

    public static void main(String[] args) throws Exception {
        new NimbusService().run(args);
    }

    @Override
    public void initialize(Bootstrap<NimbusServiceConfiguration> bootstrap) {
        bootstrap.setName("Storm Rest Service");
    }

    @Override
    public void run(NimbusServiceConfiguration config, Environment environment) throws Exception {
        LOG.info("Ganglia reporting enabled: {}", config.isEnableGanglia());
        ObjectMapperFactory factory = environment.getObjectMapperFactory();
        factory.enable(SerializationFeature.INDENT_OUTPUT);
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.NIMBUS_HOST, config.getNimbusHost());
        conf.put(Config.NIMBUS_THRIFT_PORT, config.getNimbusPort());

        if (config.getStormThriftTransport() != null) {
            conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, config.getStormThriftTransport());
        } else {
            conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "backtype.storm.security.auth.SimpleTransportPlugin");
        }
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 5);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL,2000);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 60000);

        NimbusClient nc = NimbusClient.getConfiguredClient(conf);
        environment.addResource(new NimbusResource(nc.getClient()));

        if(config.isEnableGanglia()){

            NimbusClient nc2 = NimbusClient.getConfiguredClient(conf);

            GangliaReporter reporter = new GangliaReporter(config.getGanglia(), nc2.getClient());
            reporter.start();
        }
    }
}
