------ Exploratory Data Analysis

---- Our Data
select * from 
layoffs_staging2 ;


---- Maximum total_laid_off and percentage_laid_off
select max(total_laid_off) ,max(percentage_laid_off)
from layoffs_staging2 ;

---- Companies who lay off the Most
select company , sum(total_laid_off) 
from layoffs_staging2 
group by company
order by sum(total_laid_off ) desc;
;

----  starting and ending date of layoffs
select min(`date`) , max(`date`)
from layoffs_staging2 ;

--- Which country has the most layed offs
select country , sum(total_laid_off) 
from layoffs_staging2 
group by country
order by sum(total_laid_off ) desc;
;


-- Layed offs in individual years
select Year(`date`) , sum(total_laid_off) 
from layoffs_staging2 
group by Year(`date`)
order by sum(total_laid_off ) desc;
;


--- Companies stage who have done the most layed offs
select stage , sum(total_laid_off) 
from layoffs_staging2 
group by stage
order by sum(total_laid_off ) desc;
;



select * from layoffs_staging2 ;

--- Layed offs per month
select substring(`date` , 1 ,7) as month , sum(total_laid_off)
from layoffs_staging2 
where substring(`date` , 1 ,7) is not null 
group by month
order by 1 ;


---- Rolling total of layed offs
with rolling_total as 
(
select substring(`date` , 1 ,7) as month , sum(total_laid_off) as total_off
from layoffs_staging2 
where substring(`date` , 1 ,7) is not null 
group by month
order by 1
)
select month,  total_off , sum(total_off) over(order by month) as rolling_total
from rolling_total ;

select * from layoffs_staging2 ;
select company ,Year(`date`) ,sum(total_laid_off)
from layoffs_staging2 
group by company , Year(`date`)
order by 3 desc;


--- Ranking of companies who did the most layoffs year wise
with company_year(comany , years, total_laid_off) as 
(
select company ,Year(`date`) ,sum(total_laid_off)
from layoffs_staging2 
group by company , Year(`date`)
),
company_year_rank as(
select *,
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from  company_year where years is not null
)
select * from company_year_rank 
where ranking <=5;





