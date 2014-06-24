package org.orthomcl.data.core;

public enum EdgeType {
  Ortholog("O"), Coortholog("C"), Inparalog("P"), Normal("N");
  
  private final String code;
  
  private EdgeType(String code) {
    this.code = code;
  }
  
  public String getCode() {
    return code;
  }
}
