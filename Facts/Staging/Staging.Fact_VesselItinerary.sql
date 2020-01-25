/*
==========================================================================================================
Author:			Brian Boswick
Create date:	08/16/2019
Description:	Creates the Staging.Fact_VesselItinerary table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	10/10/2019	Added DateModifiedKey column
Brian Boswick	10/20/2019	Added Original ETA benchmark metrics
Brian Boswick	10/30/2019	Added Laycan metric fields
Brian Boswick	01/25/2020	Added PortOrder field
==========================================================================================================	
*/

drop table if exists Staging.Fact_VesselItinerary;
go

create table Staging.Fact_VesselItinerary
	(
		VesselItineraryAlternateKey				int					not null,
		PostFixtureKey							int					not null,
		PortKey									int					not null,
		ETAStartDateKey							int					not null,		-- Degenerate Dimension Attributes
		ETAEndDateKey							int					not null,
		DateModifiedKey							int					not null,
		ItineraryPortType						varchar(50)			null,
		Comments								varchar(500)		null,
		NORStartDate							date				null,
		ETAOriginalDate							date				null,
		ETAOriginalCreateDate					date				null,
		TwoWeekETA								date				null,
		OneWeekETA								date				null,
		MostRecentETADate						date				null,
		ETALastModifiedDate						date				null,
		LoadDischarge							varchar(50)			null,
		NORWithinLaycanOriginal					tinyint				null,
		NORWithinLaycanFinal					tinyint				null,
		ETAWithinLaycanOriginal					tinyint				null,
		ETAWithinLaycanFinal					tinyint				null,
		PortOrder								tinyint				null,
		NORLaycanOverUnderOriginal				int					null,			-- Metrics
		NORLaycanOverUnderFinal					int					null,
		ETALaycanOverUnderOriginal				int					null,
		ETALaycanOverUnderFinal					int					null,
		DaysBetweenRecentETALastModified		smallint			null,
		DaysOutOriginalETASent					smallint			null,
		DaysOutOriginalETA						smallint			null,
		DaysOutTwoWeekETA						smallint			null,
		DaysOutOneWeekETA						smallint			null,
		ArrivedLessThanThreeDaysOriginal		tinyint				null,
		ArrivedThreeToSevenDaysOriginal			tinyint				null,
		ArrivedGreaterThanSevenDaysOriginal		tinyint				null,
		ArrivedLessThanThreeDaysTwoWeek			tinyint				null,
		ArrivedThreeToSevenDaysTwoWeek			tinyint				null,
		ArrivedGreaterThanSevenDaysTwoWeek		tinyint				null,
		ArrivedLessThanThreeDaysOneWeek			tinyint				null,
		ArrivedThreeToSevenDaysOneWeek			tinyint				null,
		ArrivedGreaterThanSevenDaysOneWeek		tinyint				null,
		NominatedQuantity						decimal(18, 2)		null,
		VesselPortStatus_Override				bit					null,
		RelatedParcelPortID						int					null,			-- ETL fields
		RelatedPortID							int					null,
		ETAChanged								bit					null,
		DateModified							date				null,
		RecordStatus							tinyint				null,
		constraint [PK_Staging_Fact_VesselItinerary_AltKey] primary key clustered 
		(
			VesselItineraryAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];