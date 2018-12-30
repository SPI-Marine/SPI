/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/18/2018
Description:	Creates the UpdateDim_Calendar stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.UpdateDim_Calendar;
go

create procedure ETL.UpdateDim_Calendar
as
begin	
	declare @CurrentDate	date,
			@ErrorMsg		varchar(1000);

	set @CurrentDate = convert(date, getdate());
	begin try
		update
				Warehouse.Dim_Calendar
			set
				RelativeMonthIndex = datediff(month, @CurrentDate, FullDate)
				,RelativeDateIndex = datediff(day, @CurrentDate, FullDate);
	
		update
				Warehouse.Dim_Calendar
			set
				RelativeMonthLabel =	case
											when RelativeMonthIndex < -1
												then convert(varchar(4), abs(RelativeMonthIndex)) + ' Months Back'
											when RelativeMonthIndex = -1
												then convert(varchar(4), abs(RelativeMonthIndex)) + ' Month Back'
											when RelativeMonthIndex = 1
												then convert(varchar(4), RelativeMonthIndex) + ' Month Ahead'
											when RelativeMonthIndex > 1
												then convert(varchar(4), RelativeMonthIndex) + ' Months Ahead'
											else 'Current Month'
										end
				,NumericYearMonth = year(FullDate) * 100 + month(FullDate)
				,IsRealizedMonth =	case
										when NumericYearMonth <= year(@CurrentDate) * 100 + month(@CurrentDate)
											then 1
										else 0
									end;
	end try
	begin catch
		select @ErrorMsg = 'Updating Dim_Calendar - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end