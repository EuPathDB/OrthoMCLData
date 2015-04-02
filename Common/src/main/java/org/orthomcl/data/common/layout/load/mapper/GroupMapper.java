package org.orthomcl.data.common.layout.load.mapper;

import java.util.List;

import org.orthomcl.data.common.layout.core.Group;

public interface GroupMapper {

  /**
   * Select groups with the # of members equal to, or less than, the given max, and the groups don't have the layout yet.
   * 
   * @param max
   * @return
   */
  List<Group> selectGroups(int max);

  Group selectGroupById(int id);

  Group selectGroupByName(String name);
  
  String selectLayout(Group group);

  /**
   * Insert the generated group layout into database.
   * 
   * @param group
   */
  int insertLayout(Group group);

  int deleteLayout(Group group);
  
  /**
   * Delete all the group layouts
   * 
   * @param max
   * @return
   */
  int deleteLayouts();
}
