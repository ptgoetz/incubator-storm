package backtype.storm.metric;

import static backtype.storm.Config.GANGLIA_HOST;
import static backtype.storm.Config.GANGLIA_MODE;
import static backtype.storm.Config.GANGLIA_PORT;
import static backtype.storm.Config.GANGLIA_REPORT_INTERVAL_SEC;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GangliaException;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Reports cluster summary metrics to Ganglia.
 * To use, add this to your topology's configuration:
 *   conf.put("ganglia.mode", "UNICAST"); //optional, default is MultiCast
 *   conf.put("ganglia.host", "host or group");
 *   conf.put("ganglia.port", 8649);//optional , default is 8649
 *   conf.put("ganglia.reportIntervalSeconds", 60);//optional , default is 60
 *
 * Or edit the storm.yaml config file:
 *
 *   ganglia.mode: UNICAST or MULTICAST //default is Multicast
 *   ganglia.host: host or group to send the ganglia event.
 *   ganglia.port: the port to send the event to. default is 8649
 *   ganglia.reportIntervalSeconds : interval in seconds to send metrics to ganglia.
 */
public class GangliaReporter {
    private static final Logger LOG = LoggerFactory.getLogger(GangliaReporter.class);

    private Timer timer;
    private GMetric ganglia;
    NimbusClient nimbusClient;

    public GangliaReporter(Map stormConf) {
        try {
            Validate.notNull(stormConf.get(GANGLIA_HOST), GANGLIA_HOST + " can not be null");

            GMetric.UDPAddressingMode mode =
                    "UNICAST".equalsIgnoreCase((String) stormConf.get(GANGLIA_MODE)) ? GMetric.UDPAddressingMode.UNICAST : GMetric.UDPAddressingMode.MULTICAST;

            this.ganglia = new GMetric(
                    stormConf.get(GANGLIA_HOST).toString(),
                    stormConf.get(GANGLIA_PORT) != null ? Integer.parseInt(stormConf.get(GANGLIA_PORT).toString()) : 8649,
                    mode,
                    1,
                    true,
                    null);
           this.nimbusClient = NimbusClient.getConfiguredClient(stormConf);
           this.timer = new Timer("ganglia-reporter", true);
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    try {
                        reportMetrics();
                    } catch (Exception e) {
                        LOG.warn("Failed to report Ganglia metrics", e);
                    }
                }
            };
            long interval = stormConf.get(GANGLIA_REPORT_INTERVAL_SEC) == null? 60 : Long.parseLong(stormConf.get(GANGLIA_REPORT_INTERVAL_SEC).toString());
            this.timer.schedule(task, interval, interval);
        } catch (Exception e) {
            LOG.warn("could not initialize ganglia, please specify ganglia.host, ganglia.port [default 8649], ganglia.mode [default MULTICAST] in your storm.yaml ", e);
        }
    }


    void reportMetrics() throws Exception {
        ClusterSummary cs = this.nimbusClient.getClient().getClusterInfo();

        if (ganglia != null) {
            reportMetric("Supervisors", cs.get_supervisors_size());
            reportMetric("Topologies", cs.get_topologies_size());

            List<SupervisorSummary> sups = cs.get_supervisors();
            int totalSlots = 0;
            int usedSlots = 0;
            for (SupervisorSummary ssum : sups) {
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
            for (TopologySummary topo : topos) {
                totalExecutors += topo.get_num_executors();
                totalTasks += topo.get_num_tasks();
            }
            reportMetric("Total Executors", totalExecutors);
            reportMetric("Total Tasks", totalTasks);
        } else {
            LOG.warn("could not emit ganglia metric as ganglia metric is not initiliazed, have you set ganglia.host, ganglia.port configs correctly?");
        }
    }


    private void reportMetric(String name, int val) throws GangliaException {
        this.ganglia.announce(name, val, "Storm");
    }
}
