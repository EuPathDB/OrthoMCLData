/**
 * 
 */
package org.apidb.orthomcl.load.plugin.biolayout;

import java.awt.Color;

/**
 * @author xingao
 *
 */
public class Node {

    public int sequenceId;
    public int taxonId;
    public String abbreviation;
    public String organism;
    public String description;
    public String sourceId;
    public int x;
    public int y;
    public Color color = Color.BLACK;
    
    public String getCombinedId() {
        return abbreviation + "|" + sourceId;
    }
}
