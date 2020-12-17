-- Práctica 2 Apache Pig. Autor: Alejandro Vizcaíno Castilla

raw = LOAD '/home/alejandro/Escritorio/RIA_exportacion_datos_diarios_Huelva_20140206.csv' USING PigStorage(';') 
AS (idprovincia:int, sprovincia:chararray, idestacion:chararray, sestacion:chararray, fecha:chararray, dia:int, tempmax:chararray, hormintempmax:chararray, tempmin:chararray, hormintempmin:chararray, 
tempmedia:chararray, humedadmax:chararray, humedadmin:chararray, humedadmedia:chararray, velviento:chararray, dirviento:chararray, radiacion:chararray, precipitacion:chararray);

raw_replaced = FOREACH raw GENERATE idprovincia, sprovincia, REPLACE(idestacion, '"','') AS idestacionnew, sestacion, fecha, dia, REPLACE(tempmax, ',','.') AS tempmaxdot, REPLACE(hormintempmax, ',','.') AS hormintempmaxdot, REPLACE(tempmin, ',','.') AS tempmindot, REPLACE(hormintempmin, ',','.') AS hormintempmindot, REPLACE(tempmedia, ',','.') AS tempmediadot, REPLACE(humedadmax, ',','.') AS humedadmaxdot, REPLACE(humedadmin, ',','.') AS humedadmindot, REPLACE(humedadmedia, ',','.') AS humedadmediadot, REPLACE(velviento, ',','.') AS velvientodot, REPLACE(dirviento, ',','.') AS dirvientodot, REPLACE(radiacion, ',','.') AS radiaciondot, REPLACE(precipitacion, ',','.') AS precipitaciondot;

fraw = FOREACH raw_replaced GENERATE idprovincia, sprovincia, (int)idestacionnew AS idestacion, sestacion, fecha, (float)radiaciondot AS radiacion, (float)precipitaciondot AS precipitacion;

clean1 = FILTER  fraw BY (idestacion ==2 OR idestacion ==3 OR idestacion ==4 OR idestacion ==5 OR idestacion ==6 OR idestacion ==7 OR idestacion ==8 OR idestacion ==9 OR idestacion ==10);

clean2 = FOREACH clean1 GENERATE sestacion, radiacion;
clean3 = GROUP clean2 BY sestacion;
clean_avg = FOREACH clean3 GENERATE FLATTEN(clean2.sestacion) AS sestacion, AVG(clean2.radiacion) AS radiacionmed;
clean_dist = DISTINCT clean_avg;
clean_max = ORDER clean_dist BY radiacionmed DESC;
STORE clean_max INTO 'parte1';

clean_ranked = rank clean_max;
top_estacion = FILTER clean_ranked BY $0==1;
municipio = foreach top_estacion generate sestacion;
clean4 = JOIN municipio BY sestacion, fraw BY sestacion;
clean5 = FOREACH clean4 GENERATE SUBSTRING(fecha, 6, 10) AS anio, precipitacion;
clean6 = GROUP clean5 BY anio;
clean7 = FOREACH clean6 GENERATE FLATTEN(clean5.anio) AS anio, SUM(clean5.precipitacion) AS precipitaciontotal;
clean8 = DISTINCT clean7;
STORE clean8 INTO 'parte2';







