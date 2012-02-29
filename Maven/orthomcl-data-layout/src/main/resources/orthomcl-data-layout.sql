CREATE TABLE SequenceLayout (
    ortholog_group_id  NUMBER(12, 0) NOT NULL,
    aa_sequence_id     NUMBER(12, 0) NOT NULL,
    layout_x           FLOAT,
    layout_y           FLOAT,
    CONSTRAINT SequenceLayout_PK PRIMARY KEY ( ortholog_group_id, aa_sequence_id )
);