/*create table addr_in
SELECT count(*) as c1 , `transaction`.`to` as c2 FROM `transaction`
GROUP BY `transaction`.`to`;
CREATE table addr1
SELECT addr.address,addr.idx,addr_in.c1,addr.`out`,addr.timeavg
FROM addr,addr_in
where addr.idx=addr_in.c2*/ /*数据预处理*/
/*create table addr_out
SELECT count(*) as a1,`transaction`.`from` as a2 FROM `transaction`
GROUP BY `transaction`.`from`;
CREATE table addr2
SELECT addr1.address,addr1.idx,addr1.c1,addr_out.a1,addr1.timeavg
FROM addr1,addr_out
WHERE addr1.idx=addr_out.a2*/
/*CREATE table timein
SELECT AVG(DISTINCT `transaction`.`Timestamp`) as b1,`transaction`.`to` as b2 
FROM `transaction`
GROUP BY `transaction`.`to`;
CREATE table addr3
SELECT addr2.address,addr2.idx,addr2.`in`,addr2.`out`,addr2.tag,timein.b1,addr2.timeoutavf
FROM addr2,timein
WHERE addr2.idx=timein.b2;*/
/*CREATE table timeout
SELECT AVG(DISTINCT `transaction`.`Timestamp`) as d1,`transaction`.`from` as d2 
FROM `transaction`
GROUP BY `transaction`.`from`
CREATE table addr4
SELECT addr3.address,addr3.idx,addr3.`in`,addr3.`out`,addr3.tag,addr3.b1,timeout.d1
FROM addr3,timeout
WHERE addr3.idx=timeout.d2;*/
/*CREATE table timestd
SELECT STD(`transaction`.`Timestamp`) as e1,`transaction`.`from` as b2
FROM `transaction`
GROUP BY `transaction`.`from`;*/
/*UPDATE addr4,timestd
set addr4.timestd=timestd.e1
WHERE addr4.idx=timestd.b2*/
/*SELECT count(*)
FROM `transaction`;
UPDATE addr4
set addr4.`in-degree-ratio`=addr4.`in`/225714;
UPDATE addr4
set addr4.`out-degree-ratio`=addr4.`out`/225714;
UPDATE addr4
SET addr4.transall=addr4.`in`+addr4.`out`;
UPDATE addr4
set addr4.`timein-out`=ABS(addr4.timeinavg-addr4.timeoutavg);
UPDATE addr4
set addr4.`in-out`=ABS(addr4.`in`-addr4.`out`);
UPDATE addr4
set addr4.`interval`=addr4.timestd/addr4.timeoutavg;*/
/*初始化*/
/*UPDATE addr4
set addr4.tag=4;*/
/*挖矿*/
/*CREATE TABLE addrmining
select `transaction`.`from` as id1,`transaction`.`to` as id2
from `transaction` 
where `transaction`.`value`*10000000000 - floor(`transaction`.`value`*10000000000) > 0*/
/*UPDATE addr4,addrmining
set addr4.tag=1
WHERE addr4.idx=addrmining.id2
OR addr4.idx=addrmining.id1
and addr4.`interval`<=0.002;*/
/*交易所*/
/*UPDATE addr4
set addr4.tag=2
WHERE addr4.`out-degree-ratio`>0.005
OR addr4.`in-degree-ratio`>0.005;*/
/*gambling*/
/*UPDATE addr4
set addr4.tag=3
where addr4.`in-out`<=2
AND addr4.transall>=10
AND addr4.`timein-out`<=200;*/
/*原表打上tag*/
/*
UPDATE addr,addr4
set addr.tag=addr4.tag
where addr.idx=addr4.idx;*/

















