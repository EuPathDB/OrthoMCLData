CREATE TABLE apidb.OrthologGroupLayout (
  ortholog_group_id NUMBER(12) NOT NULL,
  layout            CLOB,
  CONSTRAINT OrthologGroupLayout_pkey PRIMARY KEY (ortholog_group_id),
  CONSTRAINT OrthologGroupLayout_fk01 FOREIGN KEY (ortholog_group_id)
      REFERENCES apidb.OrthologGroup (ortholog_group_id)
);


GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.OrthologGroupLayout TO GUS_W;
GRANT SELECT ON apidb.OrthologGroupLayout TO GUS_R;


CREATE TABLE apidb.SimilarSequencesGroup (
  ortholog_group_id NUMBER(12) NOT NULL,
  query_id VARCHAR(60) NOT NULL,
  subject_id VARCHAR(60) NOT NULL,
  evalue_mant FLOAT,
  evalue_exp NUMBER,
  CONSTRAINT SimilarSequencesGroup_pk PRIMARY KEY (ortholog_group_id, query_id, subject_id),
  CONSTRAINT SimilarSequencesGroup_fk01 FOREIGN KEY (ortholog_group_id)
      REFERENCES apidb.OrthologGroup (ortholog_group_id)
);

CREATE INDEX apidb.SimilarSequencesGroup_ix01 ON apidb.SimilarSequencesGroup (ortholog_group_id);

GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.SimilarSequencesGroup TO GUS_W;
GRANT SELECT ON apidb.SimilarSequencesGroup TO GUS_R;


INSERT INTO apidb.SimilarSequencesGroup(ortholog_group_id, query_id, subject_id, evalue_mant, evalue_exp)
SELECT ogsq.ortholog_group_id, ss.query_id, ss.subject_id, ss.evalue_mant, ss.evalue_exp
FROM apidb.SimilarSequences ss, dots.ExternalAaSequence easq, dots.ExternalAaSequence eass, 
     APIDB.ORTHOLOGGROUPAASEQUENCE ogsq, apidb.ORTHOLOGGROUPAASEQUENCE ogss
WHERE ss.query_id = easq.secondary_identifier AND easq.aa_sequence_id = ogsq.aa_sequence_id
  AND ogsq.ortholog_group_id = ogss.ortholog_group_id
  AND ss.subject_id = eass.secondary_identifier AND eass.aa_sequence_id = ogss.aa_sequence_id;
