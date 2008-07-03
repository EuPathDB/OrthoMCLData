CREATE DATABASE apidb;
USE apidb;

CREATE TABLE apidb.SimilarSequences (
 QUERY_ID                 VARCHAR(15),
 SUBJECT_ID               VARCHAR(15),
 QUERY_TAXON_ID           VARCHAR(15),
 SUBJECT_TAXON_ID         VARCHAR(15),
 EVALUE_MANT              BIGINT(20),
 EVALUE_EXP               BIGINT(20),
 PERCENT_IDENTITY         FLOAT,
 PERCENT_MATCH            FLOAT  
);

-- GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.SimilarSequences TO kirkup;
-- GRANT SELECT ON apidb.SimilarSequences TO kirkup;

-- CREATE INDEX apidb.ss_qtaxexp_ix ON apidb.SimilarSequences(query_id, subject_taxon_id, evalue_exp, evalue_mant, query_taxon_id, subject_id);
-- CREATE INDEX apidb.ss_seqs_ix ON apidb.SimilarSequences(query_id, subject_id, evalue_exp, evalue_mant);

CREATE INDEX ss_qtaxexp_ix ON apidb.SimilarSequences(query_id, subject_taxon_id, evalue_exp, evalue_mant, query_taxon_id, subject_id);
CREATE INDEX ss_seqs_ix ON apidb.SimilarSequences(query_id, subject_id, evalue_exp, evalue_mant);


-----------------------------------------------------------

CREATE TABLE apidb.Inparalog (
 SEQUENCE_ID_A           VARCHAR(15),
 SEQUENCE_ID_B           VARCHAR(15),
 TAXON_ID                VARCHAR(15),
 UNNORMALIZED_SCORE      DOUBLE,
 NORMALIZED_SCORE        DOUBLE    
);

-- GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.Inparalog TO gus_w;
-- GRANT SELECT ON apidb.Inparalog TO gus_r;

------------------------------------------------------------

CREATE TABLE apidb.Ortholog (
 SEQUENCE_ID_A           VARCHAR(15),
 SEQUENCE_ID_B           VARCHAR(15),
 TAXON_ID_A              VARCHAR(15),
 TAXON_ID_B              VARCHAR(15),
 UNNORMALIZED_SCORE      DOUBLE,
 NORMALIZED_SCORE        DOUBLE    
);

-- CREATE INDEX ortholog_seq_a_ix on apidb.ortholog(sequence_id_a);
-- CREATE INDEX ortholog_seq_b_ix on apidb.ortholog(sequence_id_b);


-- GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.ortholog TO gus_w;
-- GRANT SELECT ON apidb.ortholog TO gus_r;

-- ----------------------------------------------------------
 
CREATE TABLE apidb.CoOrtholog (
 SEQUENCE_ID_A           VARCHAR(15),
 SEQUENCE_ID_B           VARCHAR(15),
 TAXON_ID_A              VARCHAR(15),
 TAXON_ID_B              VARCHAR(15),
 UNNORMALIZED_SCORE      DOUBLE,
 NORMALIZED_SCORE        DOUBLE    
);


-- GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.coortholog TO gus_w;
-- GRANT SELECT ON apidb.coortholog TO gus_r;


CREATE VIEW apidb.InterTaxonMatch 
	AS SELECT ss.query_id, ss.subject_id, ss.subject_taxon_id, 
	ss.evalue_mant, ss.evalue_exp 
	FROM apidb.SimilarSequences ss 
	WHERE ss.subject_taxon_id != ss.query_taxon_id;


-- GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.interTaxonMatch TO gus_w;
-- GRANT SELECT ON apidb.InterTaxonMatch TO gus_r;


-- exit;
