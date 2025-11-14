---  Data Cleaning

select * from world_layoffs.layoffs ;


------   Steps for Data Cleaning
------   1. Remove Duplicates 
------   2. Standardize the data
-------  3. Handle Null values and Blank values 
-------   4. Remove extra column or row 



-----  creating a copy of raw data layoffs , so that we can perform operation on it copy data
create table layoffs_staging
like layoffs ;


select * from layoffs_staging ;

---- Insert layoffs data into layoffs-staging 
insert into layoffs_staging
select * from layoffs ;

---- Removing Duplicate Table


select * from
(
select *,
row_number() over(Partition by company , location , industry , total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging 
) as table1 where row_num > 1 ;

--- Identify Duplicates
with cte1 as 
(
select * ,
row_number() over(Partition by company , location , 
industry, total_laid_off, percentage_laid_off, 
`date`,stage,country,funds_raised_millions 
order by Company) as row_num
from layoffs_staging 
)
select * from cte1 where Company = 'Casper' ;


--- Deleting Duplicates 
--- It did not worked, because cte does not allowed to perform a deeting operation on a table 
with duplicate_cte as 
(
select*,
row_number() over(Partition by company,
location , industry , total_laid_off, percentage_laid_off,
date,stage,country,funds_raised_millions) as  row_num
from layoffs_staging
)
delete from duplicate_cte where row_num > 1 ;

---- So what will we do now 
----- Lets create a another table just like to staging_layoffs but with adding a column row_num to it 
-----  when we have a table with row_num column , we can delete a data where row_num is greater than 1 

---- Creating a table layoffs_staging2 just like layoffs_staging but with a extra column row_num 
 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2 ;


----- Inserting data into layoffs_staging2 from layoffs_staging with an extra column row-num created with the help of window function

insert into layoffs_staging2
select *,
row_number() over(partition by company,location,
industry,total_laid_off,percentage_laid_off,date,
stage,country) as row_num
from layoffs_staging
;

----- Finding the duplicates in layoffs_staging2 table


select * from layoffs_staging2 where row_num >1 ;


------ Deleting duplicates from layoffs_staging2 

delete from layoffs_staging2 where row_num > 1 ;



-----   Standardizing the data  ( Standardizing the data means finding issues in data and fixing it )

select * from layoffs_staging2 ;

select distinct company , trim(company) from layoffs_staging2 ;

-- Trim the company column

update layoffs_staging2
set company = trim(company) ;


select * from layoffs_staging2  where industry like 'crypto%' ;

--- updating industry cryptocurrency , cryto to crypto

update layoffs_staging2 
set industry = 'Crypto' 
where industry like 'crypto%' ;



select distinct country  from layoffs_staging2  order by 1  ;



--- In the country section there are two countries 'United States' and 'Unired States.' 
----  be perform trim and trailing on 'United States.' like trim(trailing '.' from country) which resolve the problem .

select distinct country , trim(trailing '.' from country) from layoffs_staging2 order by 1 ;
update layoffs_staging2
set country= trim(trailing '.' from country)
where country like 'United States%' ;



---- date in text data type 

select `date`, str_to_date(`date` , '%m/%d/%Y') 
from layoffs_staging2 ;

update layoffs_staging2
set `date` = str_to_date(`date` ,'%m/%d/%Y') ;


select * from layoffs_staging2 ;

--- changing the data type of `date` column  to Date

alter table layoffs_staging2 
modify column `date` Date  ;




-------- Handling null values 

select * from layoffs_staging2
where total_laid_off is null
 and percentage_laid_off is null ;
 
 select * from 
 layoffs_staging2
 where industry is null
 or industry = '';
 
 select * from layoffs_staging2
 where company ='Airbnb' ;
 
 select t1.industry , t2.industry
 from layoffs_staging2 as t1
 join
 layoffs_staging2 as t2
 on
 t1.company = t2.company
 and 
 t1.location = t2.location
 where (t1.industry is null or t1.industry = '') and t2.industry is not null
 ;
 
 update layoffs_staging2
 set industry = Null 
 where industry = '' ;
 
 update 
  layoffs_staging2 as t1
 join
 layoffs_staging2 as t2
 on
 t1.company = t2.company
 set
 t1.industry = t2.industry
  where t1.industry is null  and t2.industry is not null ;

 

 


 
select * from layoffs_staging2 ;


select * from layoffs_staging2
where total_laid_off is null
 and percentage_laid_off is null ;
 
 --- Deleting the rows wheretotal_laid_off and percentage_laid_off is null
 
 delete from layoffs_staging2
where total_laid_off is null
 and percentage_laid_off is null ;
 
 select count(row_num) from layoffs_staging2 ;
 
 
 ------- Deleting a extra column row_num from working dataset
 
 
 select * from layoffs_staging2 ;
 
 alter table layoffs_staging2 drop column row_num ;
 
 -------------------------- Our data is clean ------------------
 
 



 











drop table layoffs_staging2 ;











