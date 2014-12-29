package backtype.storm.metric;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
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
import java.util.HashMap;
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
public class GangliaReporter implements IClusterReporter {
    private static final Logger LOG = LoggerFactory.getLogger(GangliaReporter.class);

    /*
     * Ganglia host name where the metrics should be sent.
    */
    public static final String GANGLIA_HOST = "host";

    /*
     * Ganglia port, default is 8649.
     */
    public static final String GANGLIA_PORT = "port";

    /*
     * Ganglia mode, unicast or multicast, default is multicast.
     */
    public static final String GANGLIA_MODE = "addressMode";

    public static final String ENABLE_GANGLIA = "enableGanglia";
    /*
     * Interval at which metrics will be sent to ganglia, default is 60 seconds.
     */
    public static final String GANGLIA_REPORT_INTERVAL_SEC = "reportInterval";

    public static final String GANGLIA = "ganglia";


    private Timer timer;
    private GMetric ganglia;
    NimbusClient nimbusClient;

    public GangliaReporter() {
    }

    public void prepare(Map conf) {
        try {
            Validate.notNull(conf.get(GANGLIA), GANGLIA + " can not be null");
            Map gangliaConf = (Map) conf.get(GANGLIA);
            GMetric.UDPAddressingMode mode =
                "UNICAST".equalsIgnoreCase((String) gangliaConf.get(GANGLIA_MODE)) ? GMetric.UDPAddressingMode.UNICAST : GMetric.UDPAddressingMode.MULTICAST;

            this.ganglia = new GMetric(
                                       gangliaConf.get(GANGLIA_HOST).toString(),
                                       gangliaConf.get(GANGLIA_PORT) != null ? Integer.parseInt(gangliaConf.get(GANGLIA_PORT).toString()) : 8649,
                                       mode,
                                       1,
                                       true,
                                       null);
            Map stormConf = Utils.readStormConfig();
            this.nimbusClient = NimbusClient.getConfiguredClient(stormConf);
        } catch (Exception e) {
            LOG.warn("could not initialize ganglia, please specify host, port [default 8649], mode [default MULTICAST] under STORM_HOME/conf/config.yaml ", e);
        }

    }


    public void reportMetrics() throws Exception {
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
            LOG.warn("could not emit ganglia metric as ganglia metric instance is null, have you set host, port configs correctly?");
        }
    }


    private void reportMetric(String name, int val) throws GangliaException {
        this.ganglia.announce(name, val, "Storm");
    }
}
