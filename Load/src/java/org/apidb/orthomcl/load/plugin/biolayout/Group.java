/**
 * 
 */
package org.apidb.orthomcl.load.plugin.biolayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xingao
 * 
 */
public class Group {

    public int groupId;
    public String name;
    public Map<Edge, Edge> edges = new LinkedHashMap<Edge, Edge>();
    public Map<Integer, Node> nodes = new LinkedHashMap<Integer, Node>();

    public Map<Integer, List<Integer>> getNodeIdsByTaxons() {
        Map<Integer, List<Integer>> taxons = new HashMap<Integer, List<Integer>>();
        for(Node node : nodes.values()) {
            List<Integer> taxon = taxons.get(node.taxonId);
            if (taxon == null) {
                taxon = new ArrayList<Integer>();
                taxons.put(node.taxonId, taxon);
            }
            taxon.add(node.sequenceId);
        }
        return taxons;
    }
}
