hive -e 'select * from cotation where cloture<>0 and ouverture<>0' | sed 's/[\t]/,/g'  > ~/cotation.csv
