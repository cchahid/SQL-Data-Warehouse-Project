/* 1. Switch to system database so we aren't locking 'DataWarehouse' */
USE master;
GO

/* 2. Force-drop the database (Kills connections and deletes everything) */
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
    PRINT 'Database deleted successfully.';
END
GO

/* 3. Recreate the Database */
CREATE DATABASE DataWarehouse;
GO

/* 4. Switch to the new database */
USE DataWarehouse;
GO

/* 5. Create Schemas (Now this will work because the DB is empty) */
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

PRINT 'Database and Schemas recreated successfully.';