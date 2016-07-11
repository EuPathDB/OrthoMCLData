package org.orthomcl.data.common.layout.core;

public class GenePair {

  protected final String queryId;
  protected final String subjectId;

  public GenePair(String queryId, String subjectId) {
    this.queryId = queryId;
    this.subjectId = subjectId;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getSubjectId() {
    return subjectId;
  }

  @Override
  public int hashCode() {
    return queryId.hashCode() ^ subjectId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof GenePair) {
      GenePair pair = (GenePair) obj;
      return (queryId.equals(pair.queryId) && subjectId.equals(pair.subjectId)) ||
          (queryId.equals(pair.subjectId) && subjectId.equals(pair.queryId));
    }
    else
      return false;
  }
}
