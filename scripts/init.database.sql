/*
CREATE DATABASE AND SCHEMAS
============================


Script Purpose:
    This creates for a new database named 'DataWareHouse' after checking it already exists.


Warning :
   Running all the scripts will drop the 'DataWareHouse' database if it's exist.
*/

USE MASTER;
GO
-- Drop and recreate the 'DataWareHouse' database
if exists (select 1 from sys.database where name = 'DataWareHouse')
begin
     alter DATABASE DataWareHouse set sigle_user with rollback immediate;
end;
go

-- Create DataWareHouse database  
create database DataWareHouse;
go
  
use Datawarehouse;

-- Create Schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
