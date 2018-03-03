bookdata_year_max = load '/BookPublish1_28/part-00000' using PigStorage (',') as (year: chararray, NoOfBooks: int);

bookdata_year_max_grp = group bookdata_year_max all;

bookdata_year_max_res = foreach bookdata_year_max_grp generate MAX(bookdata_year_max.NoOfBooks) as maximum;

res = filter bookdata_year_max  by NoOfBooks == bookdata_year_max_res.maximum ;
dump res ;

