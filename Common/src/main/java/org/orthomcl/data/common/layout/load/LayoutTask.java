package org.orthomcl.data.common.layout.load;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.orthomcl.data.common.layout.core.Group;
import org.orthomcl.shared.model.layout.GraphicsException;
import org.orthomcl.shared.model.layout.SpringLayout;

public class LayoutTask implements Runnable {

  private static final long IDLE_INTERVAL = 500;

  private final GroupFactory groupFactory;
  private final SqlSession session;

  private Group group;
  private boolean stopRequested;
  private boolean stopped;

  public LayoutTask(GroupFactory groupFactory) {
    this.groupFactory = groupFactory;
    this.session = groupFactory.openSession(ExecutorType.REUSE);
  }

  public boolean isAvailable() {
    return (group == null);
  }

  /**
   * @param group
   *          the group to set
   */
  public void setGroup(Group group) {
    this.group = group;
  }

  public void stop() {
    this.stopRequested = true;
  }

  public boolean isStopped() {
    return stopped;
  }

  @Override
  public void run() {
    stopped = stopRequested = false;
    try {
      while (!stopRequested) {
        if (group == null) { // no group is available
          try {
            Thread.sleep(IDLE_INTERVAL);
          }
          catch (InterruptedException ex) {}
        }
        else { // group available
          // load group detail
          groupFactory.loadGroupDetail(group, session);
          // create layout
          SpringLayout layout = new SpringLayout(group);
          layout.process(null);

          // save layout
          groupFactory.saveLayout(group, session);
          session.commit();

          group = null;
        }
      }
    }
    catch (GraphicsException | OrthoMCLDataException ex) {
      session.rollback();
      throw new RuntimeException(ex);
    }
    finally {
      session.close();
      stopped = true;
    }
  }
}
