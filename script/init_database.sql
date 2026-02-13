/*
=============================================================
Create Database and Schemas (Snowflake Version)
=============================================================
Script Purpose:
    This script creates a new database named 'DATABASEWAREHOUSE'.
    If the database already exists, it is dropped and recreated.
    It also creates three schemas within the database:
        - BRONZE
        - SILVER
        - GOLD

WARNING:
    Running this script will drop the entire DATABASEWAREHOUSE 
    database if it exists. All objects and data will be permanently 
    deleted. Ensure proper backups before execution.
=============================================================
*/

-- Drop database if exists
DROP DATABASE IF EXISTS DATABASEWAREHOUSE;

-- Create database
CREATE DATABASE DATABASEWAREHOUSE;

-- Use database
USE DATABASE DATABASEWAREHOUSE;

-- Create Schemas
CREATE SCHEMA BRONZE;

CREATE SCHEMA SILVER;

CREATE SCHEMA GOLD;
