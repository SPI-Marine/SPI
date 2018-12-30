/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/18/2018
Description:	Creates the Warehouse.Dim_Calendar table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_Calendar;
go

create table Warehouse.Dim_Calendar
	(
		[DateKey]				int				not null
		,[FullDate]				date			not null
		,[DayOfWeek]			tinyint			not null
		,[DayNameOfWeek]		varchar(10)		not null
		,[DayOfMonth]			tinyint			not null
		,[DayOfYear]			smallint		not null
		,[WeekdayWeekend]		varchar(10)		not null
		,[WeekOfYear]			tinyint			not null
		,[WeekOfMonth]			tinyint			not null
		,[MonthName]			varchar(10)		not null
		,[MonthOfYear]			tinyint			not null
		,[IsLastDayOfMonth]		char(1)			not null
		,[CalendarQuarter]		tinyint			not null
		,[CalendarYear]			smallint		not null
		,[CalendarYearMonth]	varchar(10)		not null
		,[CalendarYearQuarter]	varchar(10)		not null
		,[FiscalMonthOfYear]	tinyint			not null
		,[FiscalQuarter]		tinyint			not null
		,[FiscalYear]			smallint		not null
		,[FiscalYearMonth]		varchar(10)		not null
		,[FiscalYearQuarter]	varchar(10)		not null
		,[RelativeMonthIndex]	smallint		not null
		,[RelativeMonthLabel]	varchar(20)		not null
		,[RelativeDateIndex]	int				not null
		,[IsRealizedMonth]		bit				not null
		,[NumericYearMonth]		int				not null
		,[IsHoliday]			bit				not null
		,[WeekStart]			date			not null
		,[WeekEnd]				date			not null
		,constraint [PK_Dim_Calendar] primary key clustered 
		(
			[DateKey] asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary]
go
