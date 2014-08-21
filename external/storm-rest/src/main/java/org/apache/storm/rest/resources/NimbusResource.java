package org.apache.storm.rest.resources;

import backtype.storm.cluster.ClusterState;
import backtype.storm.generated.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class NimbusResource {

    private Nimbus.Client client;

    public NimbusResource(Nimbus.Client client){
        this.client = client;
    }

    @GET
    @Path("/cluster/summary")
    public Object clusterData() throws Exception {
        ClusterSummary cs = this.client.getClusterInfo();
        Map<String, Object> retval = new HashMap<String, Object>();
        retval.put("nimbus.uptime", cs.get_nimbus_uptime_secs());
        retval.put("supervisors", cs.get_supervisors_size());
        retval.put("topologies", cs.get_topologies_size());

        List<SupervisorSummary> sups = cs.get_supervisors();
        int totalSlots = 0;
        int usedSlots = 0;
        for(SupervisorSummary ssum : sups){
            totalSlots += ssum.get_num_workers();
            usedSlots += ssum.get_num_used_workers();
        }
        int freeSlots = totalSlots - usedSlots;

        retval.put("slots.total", totalSlots);
        retval.put("slots.used", usedSlots);
        retval.put("slots.free", freeSlots);

        List<TopologySummary> topos = cs.get_topologies();
        int totalExecutors = 0;
        int totalTasks = 0;
        for(TopologySummary topo : topos){
            totalExecutors += topo.get_num_executors();
            totalTasks = topo.get_num_tasks();
        }
        retval.put("executors.total", totalExecutors);
        retval.put("tasks.total", totalTasks);

        return retval;
    }
    @GET
    @Path("/supervisors/summary")
    public Object supervisorData() throws Exception {
        ClusterSummary cs = this.client.getClusterInfo();

        List<SupervisorSummary> sups = cs.get_supervisors();
        ArrayList<Map<String, Object>> retval = new ArrayList<Map<String, Object>>();
        for(SupervisorSummary sup : sups){
            Map<String, Object> sum = new HashMap<String, Object>();
            sum.put("slots.used", sup.get_num_used_workers());
            sum.put("slots.total", sup.get_num_workers());
            sum.put("host", sup.get_host());
            sum.put("id", sup.get_supervisor_id());
            sum.put("uptime", sup.get_uptime_secs());

            retval.add(sum);
        }

        return retval;
    }

    @GET
    @Path("/topology/{id}/summary")
    public Object topologyDetail(@PathParam("id")String id) throws Exception {
        HashMap<String, Object> retval = new HashMap<String, Object>();


        TopologyInfo info  = this.client.getTopologyInfo(id);



        for(ExecutorSummary execSum : info.get_executors()){
            String compId = execSum.get_component_id();

            if(!compId.startsWith("__")){
                ExecutorStats stats = execSum.get_stats();
                if(stats.get_specific().is_set_bolt()){
                    System.out.println("BOLT");
                    BoltStats bolt = stats.get_specific().get_bolt();
//                    bolt.
                }
                if(stats.get_specific().is_set_spout()){
                    System.out.println("SPOUT");
                    SpoutStats spout = stats.get_specific().get_spout();
                }

//                stats.
                System.out.println(compId + " : " + execSum.get_host() + " : " + execSum.get_port());
            }
        }

        StormTopology st = this.client.getTopology(id);
//        st.
        Map<String, Bolt> boltMap = st.get_bolts();
        Map<String, SpoutSpec> spoutMap = st.get_spouts();

//        ArrayList<Map<String, Object>> boltList = new ArrayList<Map<String, Object>>();
//        for(String boltKey : boltMap.keySet()){
//            Map<String, Object> boltInfo = new HashMap<String, Object>();
//            Bolt bolt = boltMap.get(boltKey);
//            bolt.get_bolt_object().
//        }

        return null;
    }

    @GET
    @Path("/topology/summary")
    public Object topologySummary() throws Exception {
        ClusterSummary cs = this.client.getClusterInfo();
        ArrayList<Map<String, Object>> retval = new ArrayList<Map<String, Object>>();


        List<TopologySummary> topologySummaries = cs.get_topologies();
        for(TopologySummary topo : topologySummaries){
            Map<String, Object> sum = new HashMap<String, Object>();
            sum.put("id", topo.get_id());
            sum.put("name", topo.get_name());
            sum.put("status", topo.get_status());
            sum.put("uptime", topo.get_uptime_secs());
            sum.put("tasks.total", topo.get_num_tasks());
            sum.put("workers.total", topo.get_num_workers());
            sum.put("executors.total", topo.get_num_executors());
            retval.add(sum);
        }
        return retval;
    }

    @GET
    @Path("/cluster/configuration")
    public Object getNimbusConfig() throws Exception {
        String conf = this.client.getNimbusConf();
        Object json = JSONValue.parse(conf);
        return json;
    }

}
