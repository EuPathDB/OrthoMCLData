package org.orthomcl.data;

public class GenePair {

  protected final int queryId;
  protected final int subjectId;

  public GenePair(int queryId, int subjectId) {
    this.queryId = queryId;
    this.subjectId = subjectId;
  }

  public int getQueryId() {
    return queryId;
  }

  public int getSubjectId() {
    return subjectId;
  }

  @Override
  public int hashCode() {
    return queryId ^ subjectId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof GenePair) {
      GenePair pair = (GenePair) obj;
      return (queryId == pair.queryId && subjectId == pair.subjectId) ||
          (queryId == pair.subjectId && subjectId == pair.queryId);
    }
    else
      return false;
  }
}
