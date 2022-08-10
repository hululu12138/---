use HFBase
--select distinct(filename) into filelist from Data_check_day_SQ
select * from missingData_DL order by filename, business_day
--====================================================================
declare cur1 cursor scroll
for
select * from filelist -- �������������к�Լ��list������һ���α�
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
	--��ΪҪ�ں�Լ��ʼ�ͽ���ʱ�䷶Χ��ɸѡȱʧ���ݣ�������ɸѡ�����Լ��ʼ�ͽ���ʱ��
	set @start=(select top(1) a.time from (
	select time,symbol,filename from HFBase.dbo.Data_check_day_SQ where filename=@filename
	) a  order by a.time)

	set @end=(select top(1) a.time from (
	select time from Data_check_day_SQ where filename=@filename
	) a  order by a.time desc)

	--ͨ���ļ���ɸѡ��Ӧ����Ʒ����
	set @symbol=(select top(1)a.symbol from (
	select symbol from Data_check_day_SQ where filename=@filename) a)

	--�ٽ���һ���α꣬Ϊ���ǽ����к�Լ��ȱʧ���ݵ����һ�ű�
	declare cur2 cursor scroll
	for
	--������ɸѡ�������while��ĺ�Լ��ȱʧ���ݣ������д����α�
	select *,symbol=@symbol,filename=@filename from (select * from BusinessDay 
	where business_day between dateadd(day,-1,@start) and @end) a
	left join (select time,convert(varchar(10),time,120) as tocheck from Data_check_day_SQ where filename=@filename) b
	on a.business_day=b.tocheck where time is null

	open cur2

	fetch next from cur2 into @business_day, @time, @tocheck, @symbol, @filename
	while @@FETCH_STATUS=0
	begin
	--����ָ������
		insert into missingData_SQ values(@business_day, @time, @tocheck, @symbol, @filename)
		fetch next from cur2 into @business_day, @time, @tocheck, @symbol, @filename
	end
	close cur2
	deallocate cur2

	fetch next from cur1 into @filename
end
close cur1
deallocate cur1