Create table my_project.my_dataset.my_table (my_col_str char(10), my_col_int int);

select  my_col_int + 1 from my_project.my_dataset.my_table where my_col_int > 10;