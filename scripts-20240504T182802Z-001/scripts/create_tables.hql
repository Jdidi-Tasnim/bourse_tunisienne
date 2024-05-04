CREATE TABLE IF NOT EXISTS cotation (
    seance DATE,
    groupe INT,
    code_entreprise STRING,
    nom_entreprise STRING,
    ouverture FLOAT,
    cloture FLOAT,
    plus_bas FLOAT,
    plus_haut FLOAT,
    quantite INT,
    nb_transactions INT,
    capitaux FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA INPATH '/csv_files_bourse/2021/histo_cotation_2021.csv' INTO TABLE cotation;
LOAD DATA INPATH '/csv_files_bourse/2022/histo_cotation_2022.csv' INTO TABLE cotation;
LOAD DATA INPATH '/csv_files_bourse/2023/histo_cotation_2023.csv' INTO TABLE cotation;

create table if not exists indice(
	seance DATE,
	code_indice INT,
	nom_indice string,
	indice_jour float,
	indice_veille float,
	variation_veille float,
	indice_plus_haut float,
	indice_plus_bas float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/csv_files_bourse/2021/histo_indice_2021.csv' INTO TABLE indice;
LOAD DATA INPATH '/csv_files_bourse/2022/histo_indice_2022.csv' INTO TABLE indice;
LOAD DATA INPATH '/csv_files_bourse/2023/histo_indice_2023.csv' INTO TABLE indice;
