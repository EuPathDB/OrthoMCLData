package org.orthomcl.data.layout.viewer;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Random;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JToolBar;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Logger;
import org.orthomcl.data.core.Group;
import org.orthomcl.data.layout.GraphicsException;
import org.orthomcl.data.layout.Layout;
import org.orthomcl.data.layout.SpringLayout;
import org.orthomcl.data.load.GroupFactory;
import org.orthomcl.data.load.LayoutGenerator;
import org.orthomcl.data.load.OrthoMCLDataException;
import org.orthomcl.data.load.mapper.GroupMapper;

public class GroupLayoutFrame extends JFrame {

  /**
	 * 
	 */
  private static final long serialVersionUID = 4560519543039322235L;

  public static final String[] TEST_GROUPS = { "OG5_127788", "OG5_207718", "OG5_185763", "OG5_174529",
      "OG5_167974", "OG5_162535", "OG5_154921", "OG5_149247", "OG5_139903", "OG5_136751", "OG5_134904",
      "OG5_133605", "OG5_131930", "OG5_130815", "OG5_129994", "OG5_129355", "OG5_128858", "OG5_128437",
      "OG5_128022", "OG5_127358", "OG5_127033", "OG5_126848", "OG5_126741", "OG5_126646", "OG5_126625",
      "OG5_126612", "OG5_126600", "OG5_126589", "OG5_126585" };

  private static final Logger LOG = Logger.getLogger(GroupLayoutFrame.class);

  /**
   * @param args
   * @throws OrthoMCLDataException
   */
  public static void main(String[] args) throws OrthoMCLDataException {
    GroupLayoutFrame frame = new GroupLayoutFrame();
    frame.setVisible(true);
    frame.drawGroup();
  }

  private final GroupLayoutPanel layoutPanel;
  private final JComboBox<String> cmbGroups;
  private final JCheckBox chkScoreLabel;
  private final JCheckBox chkGeneLabel;

  private final GroupFactory groupFactory;
  private Group group;

  private Layout layout;

  public GroupLayoutFrame() throws OrthoMCLDataException {
    this.layoutPanel = new GroupLayoutPanel();
    this.cmbGroups = new JComboBox<>(TEST_GROUPS);
    this.chkGeneLabel = new JCheckBox("Show Gene Info");
    this.chkScoreLabel = new JCheckBox("Show Blast Info");

    this.groupFactory = new GroupFactory(Integer.valueOf(LayoutGenerator.DEFAULT_TASK_COUNT));

    JPanel contentPanel = new JPanel(new BorderLayout());
    contentPanel.add(createToolBar(), BorderLayout.NORTH);
    contentPanel.add(layoutPanel, BorderLayout.CENTER);

    setContentPane(contentPanel);
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    setPreferredSize(new Dimension(800, 850));
    pack();

  }

  private JToolBar createToolBar() {
    JToolBar toolBar = new JToolBar();

    cmbGroups.setEditable(true);
    cmbGroups.setSelectedIndex(0);

    toolBar.add(new JLabel("Group: "));
    toolBar.add(cmbGroups);
    toolBar.add(chkGeneLabel);
    toolBar.add(chkScoreLabel);

    chkGeneLabel.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        layoutPanel.setShowGeneInfo(chkGeneLabel.isSelected());
      }
    });
    chkScoreLabel.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        layoutPanel.setShowScoreInfo(chkScoreLabel.isSelected());
      }
    });

    JButton btnDraw = new JButton("Draw");
    btnDraw.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        drawGroup();
      }
    });
    toolBar.add(btnDraw);

    JButton btnSave = new JButton("Save");
    btnSave.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        saveGroup();
      }
    });
    toolBar.add(btnSave);

    return toolBar;
  }

  private void drawGroup() {
    LOG.debug("Waiting for previous layout to stop...");
    layoutPanel.initializeBuffer();
    if (layout != null) {
      layout.cancel();
      while (!layout.isStopped()) {
        try {
          Thread.sleep(100);
        }
        catch (InterruptedException ex) {
          ex.printStackTrace();
        }
      }
    }

    try {
      String groupName = (String) cmbGroups.getSelectedItem();
      if (group == null || !group.getName().equals(groupName)) {
        LOG.debug("Loading group " + groupName);
        group = groupFactory.loadGroup(groupName);
        SqlSession session = groupFactory.openSession(ExecutorType.REUSE);
        groupFactory.loadGroupDetail(group, session);
        session.close();
      }

      Thread thread = new Thread(new Runnable() {
        public void run() {
          try {
            LOG.debug("Starting layout...");
            long start = System.currentTimeMillis();
            layout = new SpringLayout(group, new Random(0));
            layout.process(layoutPanel);
            LOG.debug("Layout finished in " + ((System.currentTimeMillis() - start) / 1000D) + " seconds.");
          }
          catch (GraphicsException ex) {
            throw new RuntimeException(ex);
          }
        }
      });
      thread.start();
    }
    catch (OrthoMCLDataException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error on Drawing", JOptionPane.ERROR_MESSAGE);
    }
  }

  private void saveGroup() {
    if (group != null) {
      SqlSession session = groupFactory.openSession(ExecutorType.SIMPLE);
      try {
        // check if the layout already exists, if so, delete it first
        GroupMapper mapper = session.getMapper(GroupMapper.class);
        String layout = mapper.selectLayout(group);
        if (layout != null)
          mapper.deleteLayout(group);

        groupFactory.saveLayout(group, session);
        session.commit();
        JOptionPane.showMessageDialog(this, "The layout of the group " + group.getName() + " is saved.",
            "Save Group", JOptionPane.INFORMATION_MESSAGE);
      }
      catch (OrthoMCLDataException ex) {
        session.rollback();
        JOptionPane.showMessageDialog(this, ex, "Error on Saving", JOptionPane.ERROR_MESSAGE);
      }
      finally {
        session.close();
      }
    }
  }
}
