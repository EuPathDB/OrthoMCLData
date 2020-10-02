# OrthoMCLData

In summary, this repository supports the OrthoMCL workflow. The repository contains Perl and Java scripts that obtain, process, and load data during the workflow.

** Dependencies

   + yarn / npm / ant
   + WEBAPP_PROP_FILE file (file with one property for the webapp target directory)
      webappTargetDir=BLAH ?????

** Installation instructions.

   + bld OrthoMCLData

** Operating instructions.

   + These scripts and plugins are run by steps in the OrthoMCL workflow XML files.

** manifest

   + OrthoMCLData/Common/src/main/java/org/orthomcl/data/common/layout :: contains Python source scripts for generation of cluster layouts
   + OrthoMCLData/Common/target/classes/mappers :: contains SQL queries need for the Python scripts to generate the cluster layouts
   + OrthoMCLData/Common/target/classes/org/orthomcl/data/common/layout :: contains the Java class files
   + OrthoMCLData/Load/bin :: contains Perl scripts used for the workflow
   + OrthoMCLData/Load/lib :: contains Perl, sql, and xml files. It's not clear that they are used anymore.
   + OrthoMCLData/Load/plugin/perl :: contains perl plugins that are vital to the workflow.
   + OrthoMCLData/Load/src/main/java/org/apidb/orthomcl/load :: contains Java scripts for the workflow, but they may be old and not used.
   + OrthoMCLData/Load/target/classes :: contains Java class files that may or may not be used.
   
   
   
