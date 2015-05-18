tajo
====

tajo-0.9.1-CDH5.2.0 Version
tajo project

sql
====

SELECT "LOTCD", "LOTID", "WAFERSEQ", "TESTID", "BIAS", "WL", "XDIEPOS", "YDIEPOS", "CALCCOLUMN", MAX("VALUE") 
  FROM (SELECT W1."LOTCD" AS "LOTCD"
             , W1."LOTID" AS "LOTID"
             , W1."WAFERSEQ" AS "WAFERSEQ"
             , W1."TESTID" AS "TESTID"
             , W1."BIAS" AS "BIAS"
             , W1."WL" AS "WL"
             , W1."XDIEPOS" AS "XDIEPOS"
             , W1."YDIEPOS" AS "YDIEPOS"
             , W1."CALCCOLUMN" AS "CALCCOLUMN"
             , W1."VALUE" AS "VALUE"
          FROM "WLCR_SUMUNPIVOT"."WLCR_SUMUNPIVOT" W1
         WHERE (
               W1."LOTCD" = '2SP'
           AND W1."PROCESS" IN ('CYCLE','NCHTB','PCHTB')
           AND W1."LOTID" = '2SPR024'
           AND W1."WAFERSEQ" IN ('1','3','7')
           AND W1."TESTID" = 'S00'
               )
       ) T 
 GROUP BY "LOTCD", "LOTID", "WAFERSEQ", "TESTID", "BIAS", "WL", "XDIEPOS", "YDIEPOS", "CALCCOLUMN"
 ORDER BY "LOTCD", "LOTID", "WAFERSEQ", "TESTID"
        , "BIAS", "WL", "XDIEPOS", "YDIEPOS", "CALCCOLUMN"
