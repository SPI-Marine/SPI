/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/18/2018
Description:	Creates the LoadDim_Calendar stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Calendar;
go

create procedure ETL.LoadDim_Calendar
	@StartDate	date,
	@EndDate	date
as
begin	
	declare @DateKey				int,
			@CurrentDate			date,
			@FullDate				date,
			@DayOfWeek				tinyint,
			@DayNameOfWeek			varchar(10),
			@DayOfMonth				tinyint,
			@DayOfYear				smallint,
			@WeekdayWeekend			varchar(10),
			@WeekOfYear				tinyint,
			@WeekOfMonth			tinyint,
			@MonthName				varchar(10),
			@MonthOfYear			tinyint,
			@IsLastDayOfMonth		char(1),
			@CalendarQtr			tinyint,
			@CalendarYear			smallint,
			@CalendarYearMonth		varchar(10),
			@CalendarYearQtr		varchar(10),
			@FiscalMonthOfYear		tinyint,
			@FiscalQtr				tinyint,
			@FiscalYear				int,
			@FiscalYearMonth		varchar(10),
			@FiscalYearQtr			varchar(10),
			@RelativeMonthIndex		smallint,
			@RelativeMonthLabel		varchar(20),
			@RelativeDateIndex		int,
			@IsRealizedMonth		bit,
			@NumericYearMonth		int,
			@IsHoliday				bit,
			@WeekStart				date,
			@WeekEnd				date,
			@DefaultDate			date,
			@UnknownStartDate		date			=	'12/30/1899',
			@UnknownEndDate			date			=	'12/31/4700';
	
	--Clear Warehouse calendar table 
	truncate table Warehouse.Dim_Calendar;
	
	begin try
		set @DefaultDate = @UnknownStartDate;
		while @DefaultDate <= @UnknownEndDate
		begin
			set @CurrentDate = convert(date, getdate());
			set @DateKey = convert(int, replace(convert(date, @DefaultDate, 112),'-',''));
			set @FullDate = @DefaultDate;
			set @DayOfWeek = datepart(weekday, @DefaultDate);
			set @DayNameOfWeek = datename(weekday, @DefaultDate);
			set @DayOfMonth = datepart(day, @DefaultDate);
			set @DayOfYear = datepart(dayofyear, @DefaultDate);
			if @DayOfWeek in (1,7)
				set @WeekdayWeekend = 'Weekend';
			else
				set @WeekdayWeekend = 'Weekday';
			set @WeekOfYear = datepart(week, @DefaultDate);
			-- Calculation: (Current Week Number of the year - Week No of First day of the Month) + 1
			set @WeekOfMonth = convert(tinyint, datepart(week, @DefaultDate)) - convert(tinyint, datepart(week, dateadd(dd, 1 - day(@DefaultDate), @DefaultDate))) + 1
			set @MonthName = datename(month, @DefaultDate);
			set @MonthOfYear = datepart(month, @DefaultDate);
			if convert(date, eomonth(@DefaultDate)) = @DefaultDate
				set @IsLastDayOfMonth = 'Y';
			else
				set @IsLastDayOfMonth = 'N';
			set @CalendarQtr = datepart(quarter, @DefaultDate);
			set @CalendarYear = year(@DefaultDate);
			if @MonthOfYear < 10
				set @CalendarYearMonth = convert(varchar(5), year(@DefaultDate)) + '-0' + CONVERT(char(1), month(@DefaultDate));
			else
				set @CalendarYearMonth = convert(varchar(5), year(@DefaultDate)) + '-' + CONVERT(char(2), month(@DefaultDate));
			set @CalendarYearQtr = convert(varchar(5), year(@DefaultDate)) + 'Q' + CONVERT(char(1), @CalendarQtr);
			if @MonthOfYear < 7
				set @FiscalMonthOfYear = @MonthOfYear + 6;
			else
				set @FiscalMonthOfYear = @MonthOfYear - 6;
			set @FiscalQtr = case 
								when @FiscalMonthOfYear < 4 then 1
								when @FiscalMonthOfYear < 7 then 2
								when @FiscalMonthOfYear < 10 then 3
								else 4
							end;
			if @MonthOfYear < 7
				set @FiscalYear = @CalendarYear;
			else
				set @FiscalYear = @CalendarYear + 1;
			if @FiscalMonthOfYear < 10
				set @FiscalYearMonth = 'FY' + convert(varchar(5), @FiscalYear) + '-0' + convert(char(1), @FiscalMonthOfYear);
			else
				set @FiscalYearMonth = 'FY' + convert(varchar(5), @FiscalYear) + '-' + convert(char(2), @FiscalMonthOfYear);
			set @FiscalYearQtr = 'FY' + convert(varchar(5), @FiscalYear) + 'Q' + convert(char(1), @FiscalQtr);
			set @RelativeMonthIndex = datediff(month, convert(date, @CurrentDate), @DefaultDate);
			set @RelativeMonthLabel =	case
											when @RelativeMonthIndex < -1
												then convert(varchar(4), abs(@RelativeMonthIndex)) + ' Months Back'
											when @RelativeMonthIndex = -1
												then convert(varchar(4), abs(@RelativeMonthIndex)) + ' Month Back'
											when @RelativeMonthIndex = 1
												then convert(varchar(4), @RelativeMonthIndex) + ' Month Ahead'
											when @RelativeMonthIndex > 1
												then convert(varchar(4), @RelativeMonthIndex) + ' Months Ahead'
											else 'Current Month'
										end;
			set @RelativeDateIndex = datediff(day, convert(date, @CurrentDate), @DefaultDate);
			set @NumericYearMonth = year(@DefaultDate) * 100 + month(@DefaultDate);
			set @IsRealizedMonth =	case
										when @NumericYearMonth <= year(@CurrentDate) * 100 + month(@CurrentDate)
											then 1
										else 0
									end;	
			set @IsHoliday =	case
									-- New Years Day
									when @WeekdayWeekend = 'WeekDay' and @MonthOfYear = 1 and @DayOfMonth = 1
										then 1
									when @DayNameOfWeek = 'Friday' and @MonthOfYear = 12 and @DayOfMonth = 31
										then 1
									when @DayNameOfWeek = 'Monday' and @MonthOfYear = 1 and @DayOfMonth = 2
										then 1
							
									-- MLK (3rd Monday of January)
									when @MonthOfYear = 1
											and @DayNameOfWeek = 'Monday'
											and @WeekOfMonth = 3
											and exists (
															select
																	1
																from
																	Warehouse.Dim_Calendar c
																where
																	@CalendarYearMonth = c.CalendarYearMonth
																	and WeekOfMonth = 1
																	and DayNameOfWeek = 'Monday'
														)
										then 1
									when @MonthOfYear = 1
											and @DayNameOfWeek = 'Monday'
											and @WeekOfMonth = 4
											and not exists (
																select
																		1
																	from
																		Warehouse.Dim_Calendar c
																	where
																		@CalendarYearMonth = c.CalendarYearMonth
																		and WeekOfMonth = 1
																		and DayNameOfWeek = 'Monday'
															)
										then 1

									-- Presidents' Day (3rd Monday of February)
									when @MonthOfYear = 2
											and @DayNameOfWeek = 'Monday'
											and @WeekOfMonth = 3
											and exists (
															select
																	1
																from
																	Warehouse.Dim_Calendar c
																where
																	@CalendarYearMonth = c.CalendarYearMonth
																	and WeekOfMonth = 1
																	and DayNameOfWeek = 'Monday'
														)
										then 1
									when @MonthOfYear = 2
											and @DayNameOfWeek = 'Monday'
											and @WeekOfMonth = 4
											and not exists (
																select
																		1
																	from
																		Warehouse.Dim_Calendar c
																	where
																		@CalendarYearMonth = c.CalendarYearMonth
																		and WeekOfMonth = 1
																		and DayNameOfWeek = 'Monday'
															)
										then 1

									-- Memorial Day (Last Monday of May)
									when @MonthOfYear = 5
											and @DayNameOfWeek = 'Monday'
											and month(dateadd(day, 7, @DefaultDate)) = 6
										then 1

									-- Fourth of July
									when @WeekdayWeekend = 'WeekDay' and @MonthOfYear = 7 and @DayOfMonth = 4
										then 1
									when @DayNameOfWeek = 'Friday' and @MonthOfYear = 7 and @DayOfMonth = 3
										then 1
									when @DayNameOfWeek = 'Monday' and @MonthOfYear = 7 and @DayOfMonth = 5
										then 1
							
									-- Labor Day (First Monday of September)
									when @MonthOfYear = 9
											and @DayNameOfWeek = 'Monday'
											and not exists	(
																select
																		1
																	from
																		Warehouse.Dim_Calendar c
																	where
																		@CalendarYearMonth = c.CalendarYearMonth
																		and c.DayNameOfWeek = 'Monday'
															)
										then 1

									-- Veterans' Day
									when @WeekdayWeekend = 'WeekDay' and @MonthOfYear = 11 and @DayOfMonth = 11
										then 1
									when @DayNameOfWeek = 'Friday' and @MonthOfYear = 11 and @DayOfMonth = 10
										then 1
									when @DayNameOfWeek = 'Monday' and @MonthOfYear = 11 and @DayOfMonth = 12
										then 1

									-- Thanksgiving Day (4th Thursday of November)
									when @MonthOfYear = 11
											and @DayNameOfWeek = 'Thursday'
											and @WeekOfMonth = 4
											and exists (
															select
																	1
																from
																	Warehouse.Dim_Calendar c
																where
																	@CalendarYearMonth = c.CalendarYearMonth
																	and WeekOfMonth = 1
																	and DayNameOfWeek = 'Thursday'
														)
										then 1
									when @MonthOfYear = 11
											and @DayNameOfWeek = 'Thursday'
											and @WeekOfMonth = 5
											and not exists (
																select
																		1
																	from
																		Warehouse.Dim_Calendar c
																	where
																		@CalendarYearMonth = c.CalendarYearMonth
																		and WeekOfMonth = 1
																		and DayNameOfWeek = 'Thursday'
															)
										then 1

									-- Christmas
									when @WeekdayWeekend = 'WeekDay' and @MonthOfYear = 12 and @DayOfMonth = 25
										then 1
									when @DayNameOfWeek = 'Friday' and @MonthOfYear = 12 and @DayOfMonth = 24
										then 1
									when @DayNameOfWeek = 'Monday' and @MonthOfYear = 12 and @DayOfMonth = 26
										then 1
									else 0
								end;
			set @WeekStart = convert(date, dateadd(day, -(datepart(weekday, @FullDate) - 1), @FullDate));
			set @WeekEnd = convert(date, dateadd(day, 7 - (datepart(weekday, @FullDate)), @FullDate));

			insert
					Warehouse.Dim_Calendar
				select	@DateKey
						,@FullDate
						,@DayOfWeek
						,@DayNameOfWeek
						,@DayOfMonth
						,@DayOfYear
						,@WeekdayWeekend
						,@WeekOfYear
						,@WeekOfMonth
						,@MonthName
						,@MonthOfYear
						,@IsLastDayOfMonth
						,@CalendarQtr
						,@CalendarYear
						,@CalendarYearMonth
						,@CalendarYearQtr
						,@FiscalMonthOfYear
						,@FiscalQtr
						,@FiscalYear
						,@FiscalYearMonth
						,@FiscalYearQtr
						,@RelativeMonthIndex
						,@RelativeMonthLabel
						,@RelativeDateIndex
						,@IsRealizedMonth
						,@NumericYearMonth
						,@IsHoliday
						,@WeekStart
						,@WeekEnd;
	
			if @DefaultDate = @UnknownStartDate
			begin
				set @DefaultDate = @StartDate;
			end
			else
				begin
					if @DefaultDate >= @StartDate and @DefaultDate < @EndDate
					begin
						set @DefaultDate = dateadd(day, 1, @DefaultDate);
					end
					else
					begin
						if @DefaultDate = @EndDate
						begin
							set @DefaultDate = @UnknownEndDate;
						end
						else
							begin
								set @DefaultDate = dateadd(day, 1, @UnknownEndDate);
							end
					end
				end
		end
	end try
	begin catch
		select error_message();
	end catch
end