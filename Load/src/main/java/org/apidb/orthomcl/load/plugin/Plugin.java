/**
 * 
 */
package org.apidb.orthomcl.load.plugin;

/**
 * @author xingao
 *
 */
public interface Plugin {

    public void setArgs(String[] args) throws Exception;
    
    public void invoke() throws Exception;
}
