/*
=======================================================
Create Database and Schemas
=======================================================
Script purpose:
	This script creates a new database named 'RetailInvt1' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up 3 schemas
	within the database: 'bronze', 'silver', 'gold'
*/

USE master;
GO

-- Drop and recreate the 'RetailInvt1' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'RetailInvt1')
BEGIN
	ALTER DATABASE RetailInvt1 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE RetailInvt1;
END;
GO

-- Create Database 'RetailInvt1'
CREATE DATABASE RetailInvt1;
GO

USE RetailInvt1;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
