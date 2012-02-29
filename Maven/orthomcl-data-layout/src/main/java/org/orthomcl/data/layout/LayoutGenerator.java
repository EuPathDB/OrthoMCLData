package org.orthomcl.data.layout;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import net.lliira.common.graphics.GraphicsException;
import net.lliira.common.graphics.layout.WeightedNetworkLayout;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.log4j.Logger;

/**
 * Hello world!
 * 
 */
public class LayoutGenerator {

    private static final String ARTIFACT_ID = "orthomcl-data-layout";

    private static final char ARG_DB_CONNECTION = 'd';
    private static final char ARG_DB_LOGIN = 'l';
    private static final char ARG_DB_PASSWORD = 'p';
    private static final char ARG_LAYOUT_TABLE = 't';
    private static final char ARG_MAX_MEMBER = 'm';

    private static final String PROP_DB_CONNECTION = ARTIFACT_ID
            + ".db.connection";
    private static final String PROP_DB_LOGIN = ARTIFACT_ID + ".db.login";
    private static final String PROP_DB_PASSWORD = ARTIFACT_ID + ".db.password";
    private static final String PROP_LAYOUT_TABLE = ARTIFACT_ID
            + ".layout.table";

    private static final String DEFAULT_LAYOUT_TABLE = "SequenceLayout";
    private static final String DEFAULT_MAX_MEMBER = "500";

    private static final Logger logger = Logger.getLogger(LayoutGenerator.class);

    public static void main(String[] args) throws IOException,
            GraphicsException {
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

        Option layoutTable = OptionBuilder.withArgName("layout-table").withDescription(
                "The database table to store the layout result. You can "
                        + "include schema in the name. Default is "
                        + DEFAULT_LAYOUT_TABLE).hasArg().create(
                ARG_LAYOUT_TABLE);
        options.addOption(layoutTable);

        Option maxMember = OptionBuilder.withArgName("number").withDescription(
                "Only process groups with number of members up to the given "
                        + "value. Default is " + DEFAULT_MAX_MEMBER).hasArg().create(
                ARG_MAX_MEMBER);
        options.addOption(maxMember);

        return options;
    }

    private final Random random;
    private final SqlSessionFactory sessionFactory;
    private final int maxMember;

    public LayoutGenerator(CommandLine commandLine) throws IOException {
        logger.info("Initializing Layout Generator...");

        random = new Random();
        maxMember = Integer.valueOf(commandLine.getOptionValue(ARG_MAX_MEMBER,
                DEFAULT_MAX_MEMBER));
        sessionFactory = createSessionFactory(commandLine);
    }

    private SqlSessionFactory createSessionFactory(CommandLine commandLine)
            throws IOException {
        Properties properties = System.getProperties();
        String dbConnection = commandLine.getOptionValue(ARG_DB_CONNECTION);
        properties.setProperty(PROP_DB_CONNECTION, dbConnection);
        String dbLogin = commandLine.getOptionValue(ARG_DB_LOGIN);
        properties.setProperty(PROP_DB_LOGIN, dbLogin);
        String dbPassword = commandLine.getOptionValue(ARG_DB_PASSWORD);
        properties.setProperty(PROP_DB_PASSWORD, dbPassword);
        String layoutTable = commandLine.getOptionValue(ARG_LAYOUT_TABLE,
                DEFAULT_LAYOUT_TABLE);
        properties.setProperty(PROP_LAYOUT_TABLE, layoutTable);

        InputStream input = Resources.getResourceAsStream(ARTIFACT_ID + ".xml");
        SqlSessionFactoryBuilder builder = new SqlSessionFactoryBuilder();
        return builder.build(input, properties);
    }

    public void process() throws GraphicsException {
        logger.info("Start processing... Max Member = " + maxMember);

        SqlSession session = sessionFactory.openSession();
        GroupMapper mapper = session.getMapper(GroupMapper.class);

        try {
            int minExp = mapper.selectMinExp();
            double maxWeight = -minExp + 1;
            logger.info("Max Weight = " + maxWeight);

            List<Group> groups = mapper.selectGroups(maxMember);
            logger.info(groups.size() + " groups loaded.");

            int count = 0;
            long loading = 0, processing = 0, saving = 0;
            for (Group group : groups) {
                long start = System.currentTimeMillis();

                if (group.getMembers() == 2) {
                    // group has only 2 sequences, always put them diagonally,
                    // and no need to read blast score.
                    loadDupletGroup(mapper, group);
                    loading += (System.currentTimeMillis() - start);
                } else { // group has 3+ sequence, use layout tool
                    loadGroup(mapper, maxWeight, group);
                    loading += (System.currentTimeMillis() - start);

                    start = System.currentTimeMillis();
                    WeightedNetworkLayout layout = new WeightedNetworkLayout(
                            group);
                    layout.setMaxWeight(maxWeight);
                    layout.process(null);
                    processing += (System.currentTimeMillis() - start);
                }

                start = System.currentTimeMillis();
                for (Sequence sequnences : group.getNodes()) {
                    mapper.insertLayout(sequnences);
                }
                session.commit();
                saving += (System.currentTimeMillis() - start);

                count++;
                if (count % 100 == 0) {
                    logger.info(count + " groups processed. loading=" + loading
                            + ", Processing=" + processing + ", Saving="
                            + saving);
                }
            }
            logger.info("Layout finished. loading=" + loading + ", Processing="
                    + processing + ", Saving=" + saving);
        } catch (GraphicsException ex) {
            session.rollback();
            throw ex;
        } finally {
            session.close();
        }
    }

    private void loadGroup(GroupMapper mapper, double maxWeight, Group group) {
        // load blast scores
        List<BlastScore> blastScores = mapper.selectBlastScores(group.getId());
        for (BlastScore score : blastScores) {
            Pair pair = new Pair(score.getIdA(), score.getIdB());
            BlastScore oldScore = group.getBlastScore(pair);

            if (oldScore == null) {
                // if the counterpart doesn't exist, don't compute weight for
                // now.
                Sequence sequenceA = group.getSequence(score.getIdA());
                if (sequenceA == null) {
                    sequenceA = new Sequence(group.getId(), score.getIdA());
                    group.addSequence(sequenceA);
                }
                score.setSequenceA(sequenceA);

                Sequence sequenceB = group.getSequence(score.getIdB());
                if (sequenceB == null) {
                    sequenceB = new Sequence(group.getId(), score.getIdB());
                    group.addSequence(sequenceB);
                }
                score.setSequenceB(sequenceB);

                group.addBlastScore(pair, score);
            } else { // compute the weight
                double weight = maxWeight
                        + (Math.log10(score.getEvalueMant()
                                * oldScore.getEvalueMant())
                                + score.getEvalueExp() + oldScore.getEvalueExp())
                        / 2;
                oldScore.setWeight(weight);
            }
        }

        // load remaining sequences
        if (group.getMembers() > group.getSequenceCount()) {
            List<Integer> sequenceIds = mapper.selectSequences(group.getId());
            for (int sequenceId : sequenceIds) {
                if (group.getSequence(sequenceId) == null)
                    group.addSequence(new Sequence(group.getId(), sequenceId));
            }
        }

        // compute weights for the single pairs
        for (BlastScore score : group.getEdges()) {
            if (score.getWeight() < 0.001) {// weight hasn't be set.
                double weight = maxWeight + Math.log10(score.getEvalueMant())
                        + score.getEvalueExp();
                score.setWeight(weight);
            }
        }
    }

    public void loadDupletGroup(GroupMapper mapper, Group group) {
        List<Integer> sequenceIds = mapper.selectSequences(group.getId());
        double x = random.nextBoolean() ? 1 : -1;
        double y = random.nextBoolean() ? 1 : -1;

        for (int sequenceId : sequenceIds) {
            Sequence sequence = new Sequence(group.getId(), sequenceId);
            sequence.setX(x);
            sequence.setY(y);
            group.addSequence(sequence);
            x = -x;
            y = -y;
        }
    }
}
