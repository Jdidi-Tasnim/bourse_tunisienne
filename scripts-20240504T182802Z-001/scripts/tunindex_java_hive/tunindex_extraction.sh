hive -e 'select * from indice where nom_indice="TUNINDEX"' | sed 's/[\t]/,/g'  > ~/tunindex.csv
