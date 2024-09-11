

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


----Staging Schema
create schema staging
;
create table staging.wildcard 
(
[1] [nvarchar](200),
[2] [nvarchar](200),
[3] [nvarchar](200),
[4] [nvarchar](200),
[5] [nvarchar](200),
[6] [nvarchar](200),
[7] [nvarchar](200),
[8] [nvarchar](200),
[9] [nvarchar](200),
[10] [nvarchar](200),
[11] [nvarchar](200),
[12] [nvarchar](200),
[13] [nvarchar](200),
[14] [nvarchar](200),
[15] [nvarchar](200),
[16] [nvarchar](200),
[17] [nvarchar](200),
[18] [nvarchar](200),
[19] [nvarchar](200),
[20] [nvarchar](200)
)