Create table my_project.dataset2.table2 (my_col_str char(10), my_col_int int);

select  my_col_int + 1 from my_project.dataset2.table2 where my_col_int > 10;