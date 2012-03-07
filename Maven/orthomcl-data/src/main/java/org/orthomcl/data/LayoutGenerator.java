package org.orthomcl.data;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.lliira.common.graphics.GraphicsException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.orthomcl.common.Gene;
import org.orthomcl.common.Group;

/**
 * Hello world!
 * 
 */
public class LayoutGenerator {

    private static final char ARG_DB_CONNECTION = 'd';
    private static final char ARG_DB_LOGIN = 'l';
    private static final char ARG_DB_PASSWORD = 'p';
    private static final char ARG_MAX_MEMBER = 'm';
    private static final char ARG_TASK_COUNT = 't';

    private static final String DEFAULT_MAX_MEMBER = "500";
    private static final String DEFAULT_TASK_COUNT = "4";

    private static final Logger logger = Logger.getLogger(LayoutGenerator.class);

    public static void main(String[] args) throws IOException,
            GraphicsException, ClassNotFoundException, SQLException {
        Options options = prepareOptions();
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            LayoutGenerator generator = new LayoutGenerator(commandLine);
            generator.process();
        } catch (ParseException ex) {
            System.err.println(ex);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("biolayout", options);
            System.exit(-1);
        }
    }

    @SuppressWarnings("static-access")
    private static Options prepareOptions() {
        Options options = new Options();

        Option dbConnection = OptionBuilder.withArgName("connection_string").withDescription(
                "Oracle JDBC connection string").isRequired().hasArg().create(
                ARG_DB_CONNECTION);
        options.addOption(dbConnection);

        Option dbLogin = OptionBuilder.withArgName("login_name").withDescription(
                "Database login user").isRequired().hasArg().create(
                ARG_DB_LOGIN);
        options.addOption(dbLogin);

        Option dbPassword = OptionBuilder.withArgName("password").withDescription(
                "Database login password").isRequired().hasArg().create(
                ARG_DB_PASSWORD);
        options.addOption(dbPassword);

        Option maxMember = OptionBuilder.withArgName("number").withDescription(
                "Only process groups with number of members up to the given "
                        + "value. Default is " + DEFAULT_MAX_MEMBER).hasArg().create(
                ARG_MAX_MEMBER);
        options.addOption(maxMember);

        Option taskCount = OptionBuilder.withArgName("number").withDescription(
                "The number of tasks to run the layout. Default is "
                        + DEFAULT_TASK_COUNT).hasArg().create(ARG_TASK_COUNT);
        options.addOption(taskCount);

        return options;
    }

    private final CommandLine commandLine;
    private final int maxMember;
    private final int taskCount;

    public LayoutGenerator(CommandLine commandLine) throws IOException,
            ClassNotFoundException, SQLException {
        logger.info("Initializing Layout Generator...");

        this.commandLine = commandLine;
        maxMember = Integer.valueOf(commandLine.getOptionValue(ARG_MAX_MEMBER,
                DEFAULT_MAX_MEMBER));
        taskCount = Integer.valueOf(commandLine.getOptionValue(ARG_TASK_COUNT,
                DEFAULT_TASK_COUNT));
        Class.forName("oracle.jdbc.OracleDriver");
    }

    private Connection openConnection(CommandLine commandLine)
            throws IOException, ClassNotFoundException, SQLException {
        String dbConnection = commandLine.getOptionValue(ARG_DB_CONNECTION);
        String dbLogin = commandLine.getOptionValue(ARG_DB_LOGIN);
        String dbPassword = commandLine.getOptionValue(ARG_DB_PASSWORD);

        Connection connection = DriverManager.getConnection(dbConnection,
                dbLogin, dbPassword);
        return connection;
    }

    public void process() throws SQLException, ClassNotFoundException,
            IOException {
        logger.info("Start processing... Max Member = " + maxMember
                + ", tasks = " + taskCount);
        Queue<Group> groups = new ConcurrentLinkedQueue<Group>();
        List<LayoutTask> tasks = new ArrayList<LayoutTask>();
        GroupFactory groupFactory = null;
        try {
            // initialize the groups & tasks.
            groupFactory = new GroupFactory(openConnection(commandLine));
            int maxWeight = groupFactory.getMaxWeight();
            logger.info("Max Weight = " + maxWeight);

            Group group = null;
            int count = 0;
            ResultSet resultSet = groupFactory.getGroupGenes(maxMember);
            for (int i = 0; i < taskCount; i++) { // start tasks
                Connection connection = openConnection(commandLine);
                LayoutTask task = new LayoutTask(groups, connection, maxWeight);
                tasks.add(task);
                new Thread(task).start();
            }

            // load & cache 2X groups than the # of tasks
            int cacheSize = taskCount * 2;
            while (resultSet.next()) {
                int groupId = resultSet.getInt("ortholog_group_id");
                boolean newGroup = false;
                if (group != null && group.getId() != groupId) {
                    // start of a next group; process the previous group
                    groups.add(group);
                    newGroup = true;
                    count++;
                    if (count % 1000 == 0)
                        logger.info(count + " groups loaded.");

                    // do not load too many groups into memory
                    while (groups.size() > cacheSize) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {}
                    }
                }
                // create a new group
                if (newGroup || group == null) group = new Group(groupId);
                // add sequence to the group
                int geneId = resultSet.getInt("aa_sequence_id");
                group.getGenes().put(geneId, new Gene(geneId));
            }
            resultSet.close();
        } finally {
            if (groupFactory != null) groupFactory.close();
            // stop tasks
            for (LayoutTask task : tasks) {
                task.stop();
            }
        }
    }
}
