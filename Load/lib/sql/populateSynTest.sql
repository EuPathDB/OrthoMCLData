-- script to populate the SynTest table, which is used to test and demo the
-- syntenicity script (which evaluates an OrthoMCL build by counting
-- ortholog-group pairs that are adjacent to each other in multiple places)

column protein_id format a12
column sequence_id format a12
column species format a12
set pagesize 50000

drop table SynTest;

create table SynTest nologging as
select lower(substr(organism, 1, 1)) || substr(organism, instr(organism, ' ') + 1, 3)
       as taxon,
       lower(substr(organism, 1, 1)) || substr(organism, instr(organism, ' ') + 1, 3)
       || '|' || source_id as protein_id,
       project_id || '-' || sequence_id || '-' || na_sequence_id as sequence_id,
       start_min as location,
       species
from ApidbTuning.GeneAttributes@eupaan
where 1 = 0;

insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphA1', 'g1', 100, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphB1', 'g1', 200, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphB2', 'g1', 300, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphB3', 'g1', 400, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphC1', 'g1', 500, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphD1', 'g1', 600, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphA2', 'g1', 700, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphB4', 'g1', 800, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphB4', 'g1', 900, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphA3', 'g1', 1000, 'Gryphon');

insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphB5', 'g2', 100, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphx',  'g2', 200, 'Gryphon');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphy',  'g2', 300, 'Gryphon');

insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('grph', 'grphC2', 'g3', 100, 'Gryphon');


insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('mino', 'minoA1', 'm1', 100, 'Minotaur');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('mino', 'minoB1', 'm1', 200, 'Minotaur');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('mino', 'minox', 'm1', 300, 'Minotaur');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('mino', 'minoC1', 'm1', 400, 'Minotaur');

insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('mino', 'minoA1', 'm2', 100, 'Minotaur');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('mino', 'minoB1', 'm2', 200, 'Minotaur');

insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('phnx', 'phnxB1', 'p1', 100, 'Phoenix');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('phnx', 'phnxx', 'p1', 200, 'Phoenix');
insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('phnx', 'phnxC1', 'p1', 300, 'Phoenix');

insert into SynTest (taxon, protein_id, sequence_id, location, species) values ('sirn', 'sirn0', 's1', 100, 'Siren');

select * from SynTest order by taxon, sequence_id, location;
