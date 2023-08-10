CREATE DATABASE IF NOT EXISTS ib;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS bronze.coletas (
    DateIns  varchar,
    Screenshot varchar,
    Available integer,
    Unavailable integer,
    SuggestedPrice float,
    FinalPrice float,
    MasterKey_RetailerProduct varchar,
    RandomPrecosNegativos float,
    RandomPrecosMissing float,
    RandomPrecosDiscrepantes float,
    RandomPrecosDiscrepantesFator float,
    FinalPricePositive float
);

CREATE TABLE IF NOT EXISTS bronze.setup (
    Customer varchar,
    Department varchar,
    Category varchar,
    Brand varchar,
    EAN varchar,
    Product varchar,
    Retailer varchar,
    MasterKey_RetailerProduct varchar
);

CREATE TABLE IF NOT EXISTS silver.coletas (
    DateIns  varchar,
    Screenshot varchar,
    Available integer,
    Unavailable integer,
    SuggestedPrice float,
    FinalPrice float,
    MasterKey_RetailerProduct varchar,
    RandomPrecosNegativos float,
    RandomPrecosMissing float,
    RandomPrecosDiscrepantes float,
    RandomPrecosDiscrepantesFator float,
    FinalPricePositive float
);

CREATE TABLE IF NOT EXISTS silver.setup (
    Customer varchar,
    Department varchar,
    Category varchar,
    Brand varchar,
    EAN varchar,
    Product varchar,
    Retailer varchar,
    MasterKey_RetailerProduct varchar
);