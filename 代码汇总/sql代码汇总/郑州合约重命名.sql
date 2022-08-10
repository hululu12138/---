use HFBase
------------------------------------------------------------------------------------
--create table ZZ_newfilename(
--time datetime not null,
--symbol varchar(10) null,
--futures_market varchar(10) not null,
--[open] float not null,
--high float not null,
--low float not null,
--[close] float not null,
--volume int not null,
--amount int not null,
--open_interest int not null,
--filename varchar(10) not null,
--new_filename varchar(10) not null
--)
----------------------------------------------------------------------------------------
declare @filename varchar(10)
declare @maturity_month varchar(5)
declare @whole_start datetime
declare @whole_end datetime
declare @maturity1 datetime
declare @maturity2 datetime
declare @n datetime
declare @year1 varchar(2)
declare @year2 varchar(2)

--select a.* into #ZZ_filter from (select distinct* from Futures_ZZ where filename like '%[0-9]') a
--select distinct filename into #ZZ_files from #ZZ_filter


--select * from #ZZ_filter order by filename,time

declare cur cursor
for
select * from #ZZ_files
open cur
fetch next from cur into @filename

while @@FETCH_STATUS=0
Begin
--set @filename='ZYCF003'
	set @maturity_month=SUBSTRING(@filename,6,7)

	set @whole_start=(select top(1) a.time from (select distinct* from Futures_ZZ where filename=@filename
	)a order by time)

	set @whole_end=(select top(1) a.time from (select distinct* from Futures_ZZ where filename=@filename
	)a order by time desc)

	set @maturity1=DATEADD(YEAR,1,@whole_start)
	set @maturity2=@whole_end
	set @n=DATEADD(year,-1,@whole_end)
	set @year1=substring(convert(varchar(4),year(@maturity1)),3,4)
	set @year2=substring(convert(varchar(4),year(@maturity2)),3,4)

	if year(@maturity2)!=year(@maturity1) or year(@whole_start)!=year(@whole_end)
	begin
		insert into  ZZ_newfilename
		select distinct*,left(@filename,4)+@year1+@maturity_month
		from #ZZ_filter
		where 
		(filename=@filename and year(time)=year(@whole_start)) or
		(filename=@filename and year(time)=year(@maturity1))
		union
		select distinct*,left(@filename,4)+@year2+@maturity_month
		from #ZZ_filter 
		where 
		(filename=@filename and year(time)=year(@whole_end)) or
		(filename=@filename and year(time)=year(@n))
		
	end
		
	else
	begin
		insert into  ZZ_newfilename
		select distinct*,left(@filename,4)+@year2+@maturity_month
		from #ZZ_filter 
		where 
		(filename=@filename and year(time)=year(@maturity1)) or
		(filename=@filename and year(time)=year(@maturity2))
	end

	print('success for '+@filename)

	fetch next from cur into @filename
end

close cur
deallocate cur

--truncate table #ZZ_newfilename
--select * from Futures_ZZ where filename=@filename

--insert into HFData.dbo.ZZ_new select * from #ZZ_newfilename order by new_filename,time