use HFBase
--select distinct(filename) into filelist from Data_check_day_SQ
select * from missingData_DL order by filename, business_day
--====================================================================
declare cur1 cursor scroll
for
select * from filelist -- 大连交易所所有合约的list，创建一个游标
open cur1

declare @start datetime
declare @end datetime
declare @time datetime
declare @tocheck datetime
declare @filename varchar(10)
declare @symbol varchar(10)
declare @business_day varchar(10)

fetch next from cur1 into @filename

while @@FETCH_STATUS=0
begin
	--因为要在合约开始和结束时间范围内筛选缺失数据，所以先筛选这个合约开始和结束时间
	set @start=(select top(1) a.time from (
	select time,symbol,filename from HFBase.dbo.Data_check_day_SQ where filename=@filename
	) a  order by a.time)

	set @end=(select top(1) a.time from (
	select time from Data_check_day_SQ where filename=@filename
	) a  order by a.time desc)

	--通过文件名筛选对应的商品代码
	set @symbol=(select top(1)a.symbol from (
	select symbol from Data_check_day_SQ where filename=@filename) a)

	--再建立一个游标，为的是将所有合约的缺失数据导入进一张表
	declare cur2 cursor scroll
	for
	--这里是筛选出在这个while里的合约的缺失数据，在其中创建游标
	select *,symbol=@symbol,filename=@filename from (select * from BusinessDay 
	where business_day between dateadd(day,-1,@start) and @end) a
	left join (select time,convert(varchar(10),time,120) as tocheck from Data_check_day_SQ where filename=@filename) b
	on a.business_day=b.tocheck where time is null

	open cur2

	fetch next from cur2 into @business_day, @time, @tocheck, @symbol, @filename
	while @@FETCH_STATUS=0
	begin
	--插入指定表里
		insert into missingData_SQ values(@business_day, @time, @tocheck, @symbol, @filename)
		fetch next from cur2 into @business_day, @time, @tocheck, @symbol, @filename
	end
	close cur2
	deallocate cur2

	fetch next from cur1 into @filename
end
close cur1
deallocate cur1