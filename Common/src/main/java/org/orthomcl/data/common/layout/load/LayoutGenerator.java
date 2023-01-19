package org.orthomcl.data.common.layout.load;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.orthomcl.data.common.layout.core.Group;

/**
 * @author Jerric
 *
 */
public class LayoutGenerator {

  private static final String ARG_MAX_MEMBER = "max";
  private static final String ARG_TASK_COUNT = "task";
  private static final String ARG_UNDO = "undo";

  public static final String DEFAULT_MAX_MEMBER = "500";
  public static final String DEFAULT_TASK_COUNT = "8";

  private static final Logger LOG = Logger.getLogger(LayoutGenerator.class);

  /**
   * @param args
   * @throws OrthoMCLDataException
   */
  public static void main(String[] args) throws OrthoMCLDataException {
    Options options = prepareOptions();
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine commandLine = parser.parse(options, args);
      LayoutGenerator generator = new LayoutGenerator(commandLine);
      if (commandLine.hasOption(ARG_UNDO)) {
        generator.undo();
      }
      else {
        generator.process();
      }
    }
    catch (ParseException ex) {
      System.err.println(ex);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("orthomclClusterLayout [-max " + DEFAULT_MAX_MEMBER +
          "] [-task " + DEFAULT_TASK_COUNT + "] [-undo]", options);
      System.exit(-1);
    }
  }

  @SuppressWarnings("static-access")
  private static Options prepareOptions() {
    Options options = new Options();

    options.addOption(Option.builder()
        .option(ARG_MAX_MEMBER)
        .desc("Only process groups with number of members up to the given value. Default is " + DEFAULT_MAX_MEMBER)
        .hasArg()
        .build());

    options.addOption(Option.builder()
        .option(ARG_TASK_COUNT)
        .desc("The number of tasks to run the layout. Default is " + DEFAULT_TASK_COUNT)
        .hasArg()
        .build());

    options.addOption(Option.builder()
        .option(ARG_UNDO)
        .desc("Remove all generated layouts from database.")
        .hasArg(false)
        .build());

    return options;
  }

  private final int maxMember;
  private final int taskCount;
  private final GroupFactory groupFactory;

  public LayoutGenerator(CommandLine commandLine) throws OrthoMCLDataException {
    LOG.info("Initializing Layout Generator...");

    maxMember = Integer.valueOf(commandLine.getOptionValue(ARG_MAX_MEMBER, DEFAULT_MAX_MEMBER));
    taskCount = Integer.valueOf(commandLine.getOptionValue(ARG_TASK_COUNT, DEFAULT_TASK_COUNT));

    this.groupFactory = new GroupFactory(taskCount);
  }

  public void undo() {
    LOG.info("Undo previous layouts...");

    int count = groupFactory.removeLayouts();

    LOG.info("Done. " + count + " layouts deleted.");
  }

  public void process() {
    LOG.info("Start processing... Max Member = " + maxMember + ", tasks = " + taskCount);

    // get groups
    List<Group> groups = groupFactory.loadGroups(maxMember);
    LOG.debug(groups.size() + " groups loaded.");

    // create task pool
    List<LayoutTask> tasks = createTaskPool();
    LOG.debug(tasks.size() + " tasks created.");

    int count = 0;
    while (!groups.isEmpty()) {
      // remove a group from the list so that we can discard it after use
      Group group = groups.remove(groups.size() - 1);
      // find an available task
      LayoutTask task = getAvailableTask(tasks);
      task.setGroup(group);
      count++;
      if (count % 100 == 0)
        LOG.debug(count + " groups processed.");
    }
    LOG.debug("Total " + count + " group processed.");

    // wait for tasks to finish
    boolean allStopped = false;
    while (!allStopped) {
      try { // wait for a bit.
        Thread.sleep(1000);
      }
      catch (InterruptedException ex) {}

      // check all tasks are finished
      allStopped = true;
      for (LayoutTask task : tasks) {
        if (!task.isStopped()) {
          // stop the task
          if (task.isAvailable())
            task.stop();
          allStopped = false;
          break;
        }
      }
    }
    LOG.info("Layout Generator finished.");
  }

  private List<LayoutTask> createTaskPool() {
    List<LayoutTask> tasks = new ArrayList<>();
    for (int i = 0; i < taskCount; i++) {
      LayoutTask task = new LayoutTask(groupFactory);
      new Thread(task).start();
      tasks.add(task);
    }
    return tasks;

  }

  /**
   * Get an available task, and blocks if no task is available.
   * 
   * @return
   */
  private LayoutTask getAvailableTask(List<LayoutTask> tasks) {
    while (true) {
      for (LayoutTask task : tasks) {
        // found an available task, return it;
        if (task.isAvailable())
          return task;
      }
      // no task is available, wait for a bit, and then check again.
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException ex) {}
    }
  }
}
