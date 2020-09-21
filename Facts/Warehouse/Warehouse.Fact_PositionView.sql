drop view if exists Warehouse.Fact_PositionView;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	09/21/2020
Description:	Creates the Warehouse.Fact_PositionView view showing data on or after 7 days ago
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create view Warehouse.Fact_PositionView
as
	select
			p.*
		from
			Warehouse.Fact_Position p (nolock)
				join Warehouse.Dim_Calendar od (nolock)
					on od.DateKey = p.OpenDateKey
		where
			od.FullDate >= convert(date, dateadd(day, -7, getdate()));
go
