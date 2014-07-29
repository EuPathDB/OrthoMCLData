package org.orthomcl.data.load;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.log4j.Logger;
import org.gusdb.fgputil.runtime.GusHome;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.orthomcl.data.core.BlastScore;
import org.orthomcl.data.core.EdgeType;
import org.orthomcl.data.core.Gene;
import org.orthomcl.data.core.GenePair;
import org.orthomcl.data.core.Group;
import org.orthomcl.data.load.mapper.GeneMapper;
import org.orthomcl.data.load.mapper.GroupMapper;

public class GroupFactory {

  private static final String PROP_DB_URL = "orthomcl.db.url";
  private static final String PROP_DB_LOGIN = "orthomcl.db.login";
  private static final String PROP_DB_PASSWORD = "orthomcl.db.password";
  private static final String PROP_DB_POOL_ACTIVE = "orthomcl.db.pool.active";
  private static final String PROP_DB_POOL_IDLE = "orthomcl.db.pool.idle";

  private static final Logger LOG = Logger.getLogger(GroupFactory.class);

  private final SqlSessionFactory sessionFactory;

  public GroupFactory(int poolSize) throws OrthoMCLDataException {
    Properties properties = loadConfig(poolSize);

    // create sessionFactory
    try {
      InputStream configStream = Resources.getResourceAsStream("orthomcl-data-mybatis.xml");
      this.sessionFactory = new SqlSessionFactoryBuilder().build(configStream, properties);
    }
    catch (IOException ex) {
      throw new OrthoMCLDataException(ex);
    }
    LOG.debug("GroupFactory initialized.");
  }

  private Properties loadConfig(int poolSize) throws OrthoMCLDataException {
    // get the model config file, and extract the connection information from it.
    String gusHome = GusHome.getGusHome();
    File configFile = new File(gusHome + "/config/gus.config");
    if (!configFile.exists())
      throw new OrthoMCLDataException("Config file is missing: " + configFile.getAbsolutePath());

    LOG.info("Loading connection info from: " + configFile.getAbsolutePath());

    try {
      // prepare properties
      Properties config = new Properties();
      config.load(new FileReader(configFile));

      Properties properties = System.getProperties();
      properties.put(PROP_DB_URL, config.getProperty("jdbcDsn"));
      properties.put(PROP_DB_LOGIN, config.getProperty("databaseLogin"));
      properties.put(PROP_DB_PASSWORD, config.getProperty("databasePassword"));
      properties.put(PROP_DB_POOL_ACTIVE, Integer.toString(poolSize));
      properties.put(PROP_DB_POOL_IDLE, Integer.toString(poolSize));

      return properties;
    }
    catch (IOException ex) {
      throw new OrthoMCLDataException(ex);
    }
  }

  public SqlSession openSession(ExecutorType type) {
    return sessionFactory.openSession(type);
  }

  public List<Group> loadGroups(int maxMemberCount) {
    SqlSession session = sessionFactory.openSession();
    GroupMapper mapper = session.getMapper(GroupMapper.class);
    List<Group> groups = mapper.selectGroups(maxMemberCount);
    session.close();
    return groups;
  }

  public Group loadGroup(String name) {
    SqlSession session = sessionFactory.openSession();
    GroupMapper mapper = session.getMapper(GroupMapper.class);
    Group group = mapper.selectGroupByName(name);
    session.close();
    return group;
  }

  public void loadGroupDetail(Group group, SqlSession session) throws OrthoMCLDataException {
    LOG.debug("Loading details for group: " + group.getName());

    GeneMapper mapper = session.getMapper(GeneMapper.class);

    // load genes
    List<Gene> genes = mapper.selectGenes(group);
    for (Gene gene : genes) {
      group.addGene(gene);
    }

    // load blast scores
    List<BlastScore> scores = mapper.selectBlastScores(group);
    for (BlastScore score : scores) {
      score.setGroup(group);
      group.addBlastScore(score);
    }

    // load edge types
    setEdgeType(group, mapper.selectOrthologs(group), EdgeType.Ortholog);
    setEdgeType(group, mapper.selectCoorthologs(group), EdgeType.Coortholog);
    setEdgeType(group, mapper.selectInparalogs(group), EdgeType.Inparalog);
  }

  private void setEdgeType(Group group, List<GenePair> edges, EdgeType type) throws OrthoMCLDataException {
    Map<GenePair, BlastScore> scores = group.getScores();
    for (GenePair edge : edges) {
      BlastScore score = scores.get(edge);
      if (score == null || score.getType() != EdgeType.Normal)
        throw new OrthoMCLDataException("Blast score doesn't exist or already have a different type than " +
            type + ": " + edge);
      score.setType(type);
    }
  }

  public void saveLayout(Group group, SqlSession session) throws OrthoMCLDataException {
    JSONObject jsLayout = new JSONObject();

    Map<String, Integer> genes = new HashMap<>();
    try {
      // output genes
      JSONArray jsGenes = new JSONArray();
      int i = 0;
      for (Gene gene : group.getGenes().values()) {
        JSONObject jsGene = gene.toJSON();
        jsGene.put("i", i); // store index of the gene
        jsGenes.put(jsGene);
        genes.put(gene.getSourceId(), i);
        i++;
      }
      jsLayout.put("N", jsGenes);

      // output scores
      JSONArray jsScores = new JSONArray();
      for (BlastScore score : group.getScores().values()) {
        // use gene index instead of sourceId to save space.
        JSONObject jsScore = score.toJSON();
        jsScore.put("Q", genes.get(score.getQueryId()));
        jsScore.put("S", genes.get(score.getSubjectId()));

        jsScores.put(jsScore);
      }
      jsLayout.put("E", jsScores);
      group.setLayout(jsLayout.toString());

      // save layout
      GroupMapper mapper = session.getMapper(GroupMapper.class);
      // insert the layout into database
      mapper.insertLayout(group);
    }
    catch (JSONException ex) {
      throw new OrthoMCLDataException(ex);
    }
  }

  public int removeLayouts() {
    SqlSession session = sessionFactory.openSession();
    try {
      GroupMapper mapper = session.getMapper(GroupMapper.class);
      int count = mapper.deleteLayouts();
      session.commit();
      return count;
    }
    finally {
      session.close();
    }
  }
}
