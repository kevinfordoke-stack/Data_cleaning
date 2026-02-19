-- Data Cleaning Project

-- View the original layoffs table
select *
from layoffs;

-- Create a staging table with the same structure as the original layoffs table
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

-- Insert all data from the original table into the staging table
Insert layoffs_staging
select *
from layoffs;

-- Check for duplicates using window function (initial attempt)
select *,
row_number() OVER(
PARTITION BY Company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
from layoffs_staging;

-- Identify duplicates using a more comprehensive partition (includes all relevant columns)
with duplicate_cte as
(
select *,
row_number() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

-- Sample check - view all records for a specific company (e.g., Casper)
select *
from layoffs_staging
where company = 'Casper';

-- Attempt to delete duplicates (this syntax won't work - creates a new staging table instead)
(
select *,
row_number() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
DELETE
from duplicate_cte
where row_num > 1;

-- Create a new staging table (layoffs_staging2) with explicit column definitions and a row_num column
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Check for any duplicate records in the new staging table
select *
from layoffs_staging2
WHERE row_num > 1;

-- Insert data from original staging table into staging2, assigning row numbers based on duplicate partitions
INSERT INTO layoffs_staging2
select *,
row_number() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

-- Delete duplicate records (keeping only the first occurrence of each duplicate set)
delete
from layoffs_staging2
WHERE row_num > 1;

-- View cleaned data after removing duplicates
select *
from layoffs_staging2;

-- Check for leading/trailing spaces in company names
select distinct company, TRIM(company)
from layoffs_staging2;

-- Remove leading/trailing spaces from all company names
update layoffs_staging2
set company = trim(company);

-- View all unique industries to check for inconsistencies
select distinct industry
from layoffs_staging2;

-- Standardize industry values - consolidate all Crypto-related entries to 'Crypto'
update layoffs_staging2
set industry = 'Crypto'
WHERE industry Like 'Crypto%';

-- Check for trailing periods in country names and display corrected versions
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

-- Remove trailing periods from country names (specifically for United States entries)
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- View all data after standardization
select *
from layoffs_staging2;

-- Check the current format of date values
select `date`
from layoffs_staging2;

-- Convert date strings from MM/DD/YYYY format to proper DATE format
UPDATE	layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y' );

-- Alter the date column data type to DATE (currently stored as text)
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Identify rows with both total_laid_off and percentage_laid_off as NULL (incomplete data)
SELECT * 
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Convert empty strings in industry column to NULL for consistency
update layoffs_staging2
set industry = null
where industry = '';

-- Find all rows with missing or empty industry values
select *
from layoffs_staging2
where industry is null
or industry = '';

-- view all records for a specific company (e.g., Airbnb)
select *
from layoffs_staging2
where company = 'Airbnb';

-- Find rows where industry is NULL and can be populated from other rows of the same company/location
select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	 on t1.company = t2.company
     and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null ;

-- Populate missing industry values by matching company and location with other records
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

-- View all data after filling missing industry values
select *
from layoffs_staging2;

-- Delete rows where both total_laid_off and percentage_laid_off are NULL (unusable records)
delete
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Final view of cleaned dataset
select *
from layoffs_staging2;

-- Remove the row_num helper column (no longer needed)
alter table layoffs_staging2
drop column row_num;
