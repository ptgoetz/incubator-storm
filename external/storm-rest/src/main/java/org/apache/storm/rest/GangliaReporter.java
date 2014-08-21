package org.apache.storm.rest;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GangliaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.*;

public class GangliaReporter {
    private static final Logger LOG = LoggerFactory.getLogger(GangliaReporter.class);

    private Timer timer;
    private Nimbus.Client client;
    private GMetric ganglia;
    private GangliaConfiguration config;

    public GangliaReporter(GangliaConfiguration config, Nimbus.Client client){
        this.client = client;
        this.config = config;
    }

    public void start() throws IOException {
        GMetric.UDPAddressingMode mode =
                this.config.isUnicast() ? GMetric.UDPAddressingMode.UNICAST : GMetric.UDPAddressingMode.MULTICAST;

        this.ganglia = new GMetric(
                this.config.getHost(),
                this.config.getPort(),
                mode,
                1,
                true,
                null,
                this.config.getSpoof());

        this.timer = new Timer("ganglia-reporter", true);

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    reportMetrics();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        LOG.info("Reporting to ganglia every {} seconds.", this.config.getReportInterval());
        long interval = this.config.getReportInterval() * 1000;
        this.timer.schedule(task, interval, interval);
    }


    void reportMetrics() throws Exception {
        ClusterSummary cs = this.client.getClusterInfo();

        reportMetric("Supervisors", cs.get_supervisors_size());
        reportMetric("Topologies", cs.get_topologies_size());

        List<SupervisorSummary> sups = cs.get_supervisors();
        int totalSlots = 0;
        int usedSlots = 0;
        for(SupervisorSummary ssum : sups){
            totalSlots += ssum.get_num_workers();
            usedSlots += ssum.get_num_used_workers();
        }
        int freeSlots = totalSlots - usedSlots;

        reportMetric("Total Slots", totalSlots);
        reportMetric("Used Slots", usedSlots);
        reportMetric("Free Slots", freeSlots);

        List<TopologySummary> topos = cs.get_topologies();
        int totalExecutors = 0;
        int totalTasks = 0;
        for(TopologySummary topo : topos){
            totalExecutors += topo.get_num_executors();
            totalTasks += topo.get_num_tasks();
        }
        reportMetric("Total Executors", totalExecutors);
        reportMetric("Total Tasks", totalTasks);
    }


    private void reportMetric(String name, int val) throws GangliaException{
            this.ganglia.announce(name, val, "Storm");
    }
}
