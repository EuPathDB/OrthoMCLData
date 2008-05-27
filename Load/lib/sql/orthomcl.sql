set timing on

prompt SimilarSequences
create table apidb.SimilarSequences as
select /*+ use_nl(sim, subject) use_nl(sim, query) */
       sim.query_id, sim.subject_id, query.taxon_id as query_taxon_id, subject.taxon_id as subject_taxon_id,
       sim.pvalue_mant as evalue_mant, pvalue_exp as evalue_exp
from dots.Similarity sim, dots.AaSequence subject, dots.AaSequence query
where sim.subject_id = subject.aa_sequence_id
  and sim.query_id = query.aa_sequence_id;

grant select on apidb.SimilarSequences to public;

prompt index SimilarSequences (query_id, subject_taxon_id, pvalue_exp, pvalue_mant, query_taxon_id, subject_id)
create index apidb.ss_qtaxexp_ix
on apidb.SimilarSequences(query_id, subject_taxon_id, pvalue_exp, pvalue_mant, query_taxon_id, subject_id);

prompt index SimilarSequences (query_id, subject_id)
create index apidb.ss_seqs_ix on apidb.SimilarSequences(query_id, subject_id, pvalue_exp, pvalue_mant);

-----------------------------------------------------------------

SimilarSequences
| InterTaxonMatch
| | BestQueryTaxonScore
| | | BestInterTaxonScore
| | | | BetterHit
| | | | | InParalog
| | | | | | InParalog2way--------|
| | | BestHit                    |
| | | | Ortholog                 |
| | | | | Ortholog2way           |
| | | | | | CoOrthologCandidate -|
| | | | | | | CoOrtholog


prompt IntertaxonMatch
-- IntertaxonMatch: different TLAs
create view apidb.IntertaxonMatch as
select ss.query_id, ss.subject_id, ss.subject_taxon_id, ss.evalue_mant, ss.evalue_exp
from apidb.SimilarSequences ss
where ss.subject_taxon_id != ss.query_taxon_id;

prompt BestQueryTaxonScore
-- BestQueryTaxonScore: score of best p-value between each protein and each
--                      taxon (other than its own).   Only similarities
--                      with this p-value can participate in ortholog links.
--                      assumes that all SimilarSequence records with mantissa = 0
--                      have exponent less than any record with mantissa > 0
--                      (implying a lexocgraphic order on (exponent, mantissa) )
create table apidb.BestQueryTaxonScore as
select im.query_id, im.subject_taxon_id, low_exp.evalue_exp, min(im.evalue_mant) as evalue_mant
from apidb.IntertaxonMatch im,
     (select query_id, subject_taxon_id, min(evalue_exp) as evalue_exp
      from apidb.IntertaxonMatch
      group by query_id, subject_taxon_id) low_exp
where im.query_id = low_exp.query_id
  and im.subject_taxon_id = low_exp.subject_taxon_id
  and im.evalue_exp = low_exp.evalue_exp
group by im.query_id, im.subject_taxon_id, low_exp.evalue_exp;

prompt index BestQueryTaxonScore (query_id, subject_taxon_id, evalue_exp, evalue_mant)
create index apidb.qtscore_ix on apidb.BestQueryTaxonScore(query_id, subject_taxon_id, evalue_exp, evalue_mant);

prompt BestIntertaxonScore
-- BestIntertaxonScore: score of best p-value between each protein and any
--                      protein from any taxon other than its own.  This score
--                      is the cutoff for inParalog links
-- For the sake of performance, this uses BestQueryTaxonScore as a proxy for
-- IntertaxonMatch (it's much smaller and has all the evalues that matter)
create table apidb.BestIntertaxonScore as
select im.query_id, low_exp.evalue_exp, min(im.evalue_mant) as evalue_mant
from apidb.BestQueryTaxonScore im,
     (select query_id, min(evalue_exp) as evalue_exp
      from apidb.BestQueryTaxonScore
      group by query_id) low_exp
where im.query_id = low_exp.query_id
  and im.evalue_exp = low_exp.evalue_exp
group by im.query_id, low_exp.evalue_exp;


prompt BetterHit
-- BetterHit: intrataxon similarity with p-value <= BestIntertaxonScore. . .
create table apidb.BetterHit as
select s.query_id, s.subject_id,
       s.query_taxon_id as taxon, 
       s.evalue_exp, s.evalue_mant
from apidb.SimilarSequences s, apidb.BestIntertaxonScore bis
where s.query_id != s.subject_id
  and s.query_taxon_id = s.subject_taxon_id
  and s.query_id = bis.query_id
  and (s.evalue_mant < 0.001
       or s.evalue_exp < bis.evalue_exp
       or (s.evalue_exp = bis.evalue_exp and s.evalue_mant <= bis.evalue_mant))
-- . . . or Similarity for a protein with no BestIntertaxonScore
--       (i.e. an intrataxon match for a protein with no intertaxon
--        match in the database)
union
select s.query_id, s.subject_id, s.evalue_exp, s.evalue_mant
from apidb.SimilarSequences s
where s.query_taxon_id = s.subject_taxon_id
  and s.query_id in (select query_id from apidb.SimilarSequences
                     minus select query_id from apidb.BestIntertaxonScore);

prompt InParalog
-- InParalog: (A, B) and (B, A) are both BetterHits, and A < B
create table apidb.InParalog as
select bh1.query_id as aa_sequence_id_a, bh1.subject_id as aa_sequence_id_b, bh1.taxon
       case -- don't try to calculate log(0) -- use rigged exponents of SimSeq
         when bh1.evalue_mant < 0.01 or bh2.evalue_mant < 0.01
           then (bh1.evalue_exp + bh2.evalue_exp) / -2
         else  -- score = ( -log10(evalue1) - log10(evalue2) ) / 2
           (log(10, bh1.evalue_mant * bh2.evalue_mant)
            + bh1.evalue_exp + bh2.evalue_exp) / -2
       end as unnormalized_score,
       cast(null as number) as normalized_score
from apidb.BetterHit bh1, apidb.BetterHit bh2
where bh1.query_id < bh1.subject_id
  and bh1.query_id = bh2.subject_id
  and bh1.subject_id = bh2.query_id;

prompt BestHit
-- BestHit: Similarity with p-value = BestQueryTaxonScore. . .

create table apidb.BestHit as
select s.query_id, s.subject_id,
       s.query_taxon_id, s.subject_taxon_id,
       s.evalue_exp, s.evalue_mant
from apidb.SimilarSequences s, apidb.BestQueryTaxonScore cutoff
where s.query_id = cutoff.query_id
  and s.subject_taxon_id = cutoff.subject_taxon_id
  and s.query_taxon_id != s.subject_taxon_id
  and (s.evalue_mant < 0.01
       or s.evalue_exp = cutoff.evalue_exp
          and s.evalue_mant = cutoff.evalue_mant);

  -- OK to test floats for equality?


prompt Ortholog
-- Ortholog: symmetric pairs from BestHit.  Score calculation assumes that
--           evalues with a mantissa of zero have had their exponent set to
--           one less than the otherwise min.
create table apidb.Ortholog as
select bh1.query_id as aa_sequence_id_a, bh1.subject_id as aa_sequence_id_b,
       bh1.query_taxon_id as taxon_id_a, bh1.subject_taxon_id as taxon_id_b,
       case -- don't try to calculate log(0) -- use rigged exponents of SimSeq
         when bh1.evalue_mant < 0.01 or bh2.evalue_mant < 0.01
           then (bh1.evalue_exp + bh2.evalue_exp) / -2
         else  -- score = ( -log10(evalue1) - log10(evalue2) ) / 2
           (log(10, bh1.evalue_mant * bh2.evalue_mant)
            + bh1.evalue_exp + bh2.evalue_exp) / -2
       end as unnormalized_score,
       cast(null as number) as normalized_score
from apidb.BestHit bh1, apidb.BestHit bh2
where bh1.query_id < bh1.subject_id
  and bh1.query_id = bh2.subject_id
  and bh1.subject_id = bh2.query_id;

prompt ortholog2way

create table apidb.ortholog2way as
-- symmetric closure of Ortholog
select aa_sequence_id_a, aa_sequence_id_b from apidb.Ortholog
union
select aa_sequence_id_b as aa_sequence_id_a, aa_sequence_id_a as aa_sequence_id_b from apidb.Ortholog;

prompt inparalog2way
-- symmetric closure of InParalog
create table apidb.inparalog2way as
select aa_sequence_id_a, aa_sequence_id_b from apidb.Inparalog
union
select aa_sequence_id_b as aa_sequence_id_a, aa_sequence_id_a as aa_sequence_id_b from apidb.Inparalog;

prompt index inparalog2way (a, b)
create index apidb.in2a_ix on apidb.inparalog2way(aa_sequence_id_a, aa_sequence_id_b);

prompt index inparalog2way (b, a)
create index apidb.in2b_ix on apidb.inparalog2way(aa_sequence_id_b, aa_sequence_id_a);

prompt coorthologCandidate
-- 
create table apidb.CoorthologCandidate as
select distinct
       least(aa_sequence_id_a, aa_sequence_id_b) as aa_sequence_id_a,
       greatest(aa_sequence_id_a, aa_sequence_id_b) as aa_sequence_id_b
from (-- twp-way union, part 1: inparalog-ortholog-inparalog
      select ip1.aa_sequence_id_a, ip2.aa_sequence_id_b
      from apidb.inparalog2way ip1, apidb.ortholog2way o, apidb.inparalog2way ip2
      where ip1.aa_sequence_id_b = o.aa_sequence_id_a
        and o.aa_sequence_id_b = ip2.aa_sequence_id_a
   union -- part 2: ortholog-inparalog
      select ip.aa_sequence_id_a, o.aa_sequence_id_b
      from apidb.inparalog2way ip, apidb.ortholog2way o
      where ip.aa_sequence_id_b = o.aa_sequence_id_a);

prompt coortholog
-- CandidateCoortholog s.t. (A, B) and (B, A) are SimilarSequences and A < B
create table apidb.coortholog as
select /*+ use_nl(candidate, ab) use_nl(candidate, ba)  */
       candidate.aa_sequence_id_a, candidate.aa_sequence_id_b,
       case  -- in case of 0 evalue, use rigged exponent
         when ab.evalue_mant < 0.00001 or ba.evalue_mant < 0.00001
           then (ab.evalue_exp + ba.evalue_exp) / -2
         else -- score = ( -log10(evalue1) - log10(evalue2) ) / 2
           (log(10, ab.evalue_mant * ba.evalue_mant)
            + ab.evalue_exp + ba.evalue_exp) / -2
       end as score
from apidb.SimilarSequences ab, apidb.SimilarSequences ba,
     (select aa_sequence_id_a, aa_sequence_id_b from apidb.CoorthologCandidate
      minus select aa_sequence_id_a, aa_sequence_id_b from apidb.Ortholog) candidate
where ab.query_id = candidate.aa_sequence_id_a
  and ab.subject_id = candidate.aa_sequence_id_b
  and ba.query_id = candidate.aa_sequence_id_b
  and ba.subject_id = candidate.aa_sequence_id_a;

exit
