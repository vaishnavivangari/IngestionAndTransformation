USE org;

CREATE TABLE IF NOT EXISTS transformation_result(
employee_id int(50) NOT NULL PRIMARY KEY,
first_name varchar(200),
salary int(50),
department_id int(50),
row_created_timestamp timestamp,
rownumber int(50),
salary_rank int(50),
salary_dense_rank int(50),
salary_per_rank int(50),
department_name varchar(200),
manager_id int(50)
);




