DROP DATABASE IF EXISTS org;

CREATE DATABASE IF NOT EXISTS org;

USE org;

CREATE TABLE IF NOT EXISTS employee(
employee_id int(50) NOT NULL PRIMARY KEY,
first_name varchar(200),
last_name varchar(200),
email varchar(200),
salary int(50),
manager_id int(50),
department_id int(50)
);

CREATE TABLE IF NOT EXISTS department(
department_id int(50) NOT NULL PRIMARY KEY,
department_name varchar(200),
manager_id int(50) ,
location_id int(50)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/emp.csv'
INTO TABLE employee
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dept.csv'
INTO TABLE department
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';


