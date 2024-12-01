
/*ham veri üzerinde işlem yapmamak için öncelikle kopyası alınıyor. yeni bir kopya tablo oluştururken datasette unique bir alan olmadığından
data cleaning işlemlerinde yardımcı olması açısından row_num alanı kopya dataset'e eklenmiştir  
yaş değişkeni ile ilgili sayısal işlemler yapılabilmesi amacıyla veri tipi integer olan alanlar tabloya eklenecektir.*/

drop table if exists copy_data;
CREATE TABLE `copy_data` (
  `age` text,
  `industry` text,
  `job_title` text,
  `additional_information_about_job_title` text,
  `annual_salary` text,
  `additional_compensation` text,
  `currency` text,
  `other_currency` text,
  `additional_information_about_income` text,
  `country` text,
  `state` text,
  `city` text,
  `total_experience` text,
  `current_job_experience` text,
  `education` text,
  `gender` text,
  `race` text,
  `min_age` int,
  `max_age` int,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into copy_data
select *, 
case 
		when age = 'under 18' then right(age, 2)
	else
		left(age, 2)
	end as min_age,
    case
		when age = '65 or over' then  left(age, 2)
	else
		right(age, 2)
	end as max_age,

row_number() over() from raw_data;

/* 18 yaş altında teacher, doctor gibi gerçeklikten kopuk meslekler yer almaktadır. bu türdeki veriler temizlenecektir.
aşağıdaki sorguda 18 yaş altında olan kişilerin mesleklerinin yıllık ortalama ne kadar kazandıkları gösterilmekteidir. hem yaş açısından 
hemde kazanılan ortalama maaş, toplam tecrübe açısından gerçekliği yansıtmadığı görülmektedir.*/

select age, job_title, avg(annual_salary) 
from copy_data 
where job_title in (select job_title from copy_data where age = 'under 18') and age != 'under 18'
group by age, job_title;

/*CTE yardımıyla walmart cashier, intern ve mcdonalds crew member dışındakiler uçuruldu*/
with cte as 
(
select age, job_title, annual_salary, row_num from copy_data 
where age = 'under 18' and job_title in (select distinct job_title  from copy_data where age = 'under 18')
and job_title not in ('Walmart cashier', 'Intern', 'McDonalds Crew Member')
)
delete from copy_data
where row_num in (select row_num from cte);

select * from copy_data where age = 'under 18';
-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------
/* duplicate verilerin olup olmadığı kontol edilecektir.*/

show columns from copy_data;

with dense as
(
select
*,
dense_rank() over(partition by age,industry,job_title,additional_information_about_job_title,annual_salary,
additional_compensation,currency,other_currency,additional_information_about_income,
country,state,city,total_experience,current_job_experience,education,gender,race
 ) as dense_ranking
from copy_data
)
select * from dense where dense_ranking > 1;
/* kontrol sonucunda tekrar eden verilere rastlanılmamıştır.*/

-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------
/*verilerin standartlaştırma işlemleri yapılmaktadır.*/

update copy_data
set industry = upper(industry), 
	job_title = upper(job_title),
    country = upper(country),
	state = upper(state),
	city = upper(city),
    education = upper(education),
    gender = upper(gender),
    race = upper(race);

update copy_data
set country = trim(country);

/* country değikeninde yer alan verilerden karakter uzunluklukları 20 ve üzerinde olan veriler silinecektir. silme dışında ortak başka bir değer de atanabilir*/


delete from copy_data
where length(country) >= 20;

delete from copy_data
where length(country) < 5;

/* country değişkenindeki ayni bilgiyi işaret eden veriler temizlenecektir*/


update copy_data
set country = 'AMERICA'
where country in('United States','US','USA','U.S.',
'United States of America','U.S>','U.S.A','U.S.A.','America',
'The United States','United State of America','United Stated',
'UNITED STATES','USA-- Virgin Islands','United Statws','U.S','Unites States',
'U. S.','United Sates','United States of American','Uniited States',
'United Sates of America','Unted States','United Statesp','United Stattes',
'United Statea','United Statees','Uniyed states','Uniyes States','United States of Americas','US of A','U.SA','United Status',
'Uniteed States','United Stares','Unite States','The US','UnitedStates','United statew','United Statues','Untied States','USAB','Unitied States',
'United Sttes','USA tomorrow','United Stateds','Unitef Stated','United States- Puerto Rico','USD','United Statss'
);

			
update copy_data
set country = 'UNITED KINGDOM'
where country in('United Kingdom','UK','United Kingdom.','U.K.','United Kindom',
'UK (Northern Ireland)','UK for U.S. company','United Kingdomk','England, Gb',
'U.K. (northern England)','U.K','England, United Kingdom','Englang','UK (England)',
'UK, remote','Scotland, UK','Unites kingdom'
);


update copy_data 
set country = 'CANADA'
WHERE COUNTRY LIKE '%nada' or COUNTRY LIKE 'cana%';

/*industry değişkeninde yar alan 100 ün altındaki veriler 'other' olarak değiştirilecektir.*/

with cte as
(
select 
count(industry) as count_of, 
industry from copy_data
group by industry
)
update copy_data
set industry = 'OTHER'
where industry in (select industry from cte where count_of < 100);

/*job_title değişkeninde yar alan 11 in altındaki veriler 'other' olarak değiştirilecektir.*/
with cte_job as
(
select 
count(job_title) as count_of, 
job_title from copy_data 
group by job_title
)
update copy_data
set job_title = 'OTHER'
where job_title in (select job_title from cte_job where count_of <= 10);

alter table copy_data
drop column additional_information_about_job_title; 

alter table copy_data
drop column additional_information_about_income;

select 
count(job_title) as count_of, 
job_title from copy_data 
group by job_title;

/*son halini yeni bir tablo oluşturup aktırımı yapılıyor*/

create table final_data
like copy_data;


insert into final_data
select * from copy_data;


select * from final_data;

