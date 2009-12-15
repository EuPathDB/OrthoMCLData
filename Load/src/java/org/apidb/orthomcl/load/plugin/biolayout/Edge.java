/**
 * 
 */
package org.apidb.orthomcl.load.plugin.biolayout;

/**
 * @author xingao
 * 
 */
public class Edge {

    public String queryId;
    public String subjectId;
    public EdgeType type = EdgeType.Normal;
    public double evalueMant;
    public int evalueExp;
    public double weight;

    public Edge() {}

    public Edge(String queryId, String subjectId) {
        this.queryId = queryId;
        this.subjectId = subjectId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof Edge) {
            Edge edge = (Edge) obj;
            return (queryId.equals(edge.queryId) && subjectId.equals(edge.subjectId))
                    || (queryId.equals(edge.subjectId) && subjectId.equals(edge.queryId));
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return queryId.hashCode() ^ subjectId.hashCode();
    }
}
