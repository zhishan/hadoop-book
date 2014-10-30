grunt> A = load '/home/zhishan/workspace/git/hadoop-book/input/pig/join/A' Using PigStorage('\t') AS (a:bytearray, b:chararray) ;
grunt> describe A;
A: {a: bytearray,b: chararray}
runt> describe B
B: {a: chararray,b: bytearray}

    "set" ...
  "explain" ...
  "illustrate" ...

grunt> jn = join A by a, B by b;

grunt> describe jn;
jn: {A::a: bytearray,A::b: chararray,B::a: chararray,B::b: bytearray}
grunt> illustrate jn;
-------------------------------------------
| A     | a:bytearray    | b:chararray    | 
-------------------------------------------
|       | 4              | Coat           | 
|       | 4              | Coat           | 
-------------------------------------------
-------------------------------------------
| B     | a:chararray    | b:bytearray    | 
-------------------------------------------
|       | Hank           | 4              | 
|       | Hank           | 4              | 
-------------------------------------------
------------------------------------------------------------------------------------------
| jn     | A::a:bytearray    | A::b:chararray    | B::a:chararray    | B::b:bytearray    | 
------------------------------------------------------------------------------------------
|        | 4                 | Coat              | Hank              | 4                 | 
|        | 4                 | Coat              | Hank              | 4                 | 
|        | 4                 | Coat              | Hank              | 4                 | 
|        | 4                 | Coat              | Hank              | 4                 | 
------------------------------------------------------------------------------------------

grunt> dump A 
(2,Tie,Joe,2)
(2,Tie,Hank,2)
(3,Hat,Eve,3)
(4,Coat,Hank,4)
grunt> describe jn;
jn: {A::a: bytearray,A::b: chararray,B::a: chararray,B::b: bytearray}
grunt> A_IN_B = foreach jn generate A::a as a, A::b as b;
grunt> describe A_IN_B;                                           
A_IN_B: {a: bytearray,b: chararray}
grunt> tmp = group A_IN_B by a;
grunt> describe tmp
tmp: {group: bytearray,A_IN_B: {(a: bytearray,b: chararray)}}
grunt> A_IN_B_CNT = foreach tmp generate group, $1.a; 
grunt> dump A_IN_B_CNT
(2,{(2),(2)})
(3,{(3)})
(4,{(4)})
grunt> A_IN_B_CNT = foreach (group A_IN_B by a) { generate group, count($1) as c};
grunt> A_IN_B_CNT = foreach (group A_IN_B by a) { generate group as a, count($1) as c; } 
2014-10-30 23:18:31,925 [main] ERROR org.apache.pig.tools.grunt.Grunt - ERROR 1070: Could not resolve count using imports: [, java.lang., org.apache.pig.builtin., org.apache.pig.impl.builtin.]
Details at logfile: /home/zhishan/workspace/git/hadoop-book/input/pig/join/pig_1414681998642.log
http://pig.apache.org/docs/r0.12.1/basic.html#foreach
