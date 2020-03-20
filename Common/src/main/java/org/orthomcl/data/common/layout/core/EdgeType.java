package org.orthomcl.data.common.layout.core;

public enum EdgeType {
    Ortholog("O"), Coortholog("C"), Inparalog("P"), PeripheralCore("L"), PeripheralPeripheral("M"), Normal("N");
  
  private final String code;
  
  private EdgeType(String code) {
    this.code = code;
  }
  
  public String getCode() {
    return code;
  }
}
