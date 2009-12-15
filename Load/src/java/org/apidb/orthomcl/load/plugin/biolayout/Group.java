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
    public Map<String, Node> nodes = new LinkedHashMap<String, Node>();

    public Map<Integer, List<String>> getNodeIdsByTaxons() {
        Map<Integer, List<String>> taxons = new HashMap<Integer, List<String>>();
        for (Node node : nodes.values()) {
            List<String> taxon = taxons.get(node.taxonId);
            if (taxon == null) {
                taxon = new ArrayList<String>();
                taxons.put(node.taxonId, taxon);
            }
            taxon.add(node.getCombinedId());
        }
        return taxons;
    }
}
