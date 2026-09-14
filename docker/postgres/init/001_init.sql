CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

\ir bronze/001_init_bronze.sql
\ir silver/001_init_silver.sql
