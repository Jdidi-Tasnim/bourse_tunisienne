--Load the CSV file
data = LOAD '/data_csv/cotation.csv' USING PigStorage(',') 
       AS (seance:chararray, groupe:int, code_entreprise:chararray, nom_entreprise:chararray, 
           ouverture:float, cloture:float, plus_bas:float, plus_haut:float, 
           quantite:int, nb_transactions:int, capitaux:float);

--Calculate the moving average
data_grouped = GROUP data BY nom_entreprise;
moving_average = FOREACH data_grouped GENERATE group AS nom_entreprise, AVG(data.cloture) AS moving_avg;

--Store the result
STORE moving_average INTO 'moving_average_1.csv' USING PigStorage(',');
