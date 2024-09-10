create schema challenge
;
create table challenge.departments 
(
id int primary key,
department varchar(256)
)
;
create table challenge.jobs 
(
id int primary key,
job varchar(256)
)
;
create table challenge.jobs 
(
id int primary key,
job varchar(256)
)
;
create table challenge.hired_employees
(
id int primary key,
[name] varchar(256),
[datetime] varchar(256),
department_id integer foreign key references challenge.departments(id),
job_id integer foreign key references challenge.jobs(id) 
)


