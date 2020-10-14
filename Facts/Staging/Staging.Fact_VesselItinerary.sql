drop table if exists Staging.Fact_VesselItinerary;
go

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
Brian Boswick	02/05/2020	Added ChartererKey and OwnerKey
Brian Boswick	02/11/2020	Added VesselKey
Brian Boswick	02/21/2020	Added Direction and ProductType
Brian Boswick	05/06/2020	Added VesselPortStatusOfficial
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	10/12/2020	Added ETAEndOriginal
==========================================================================================================	
*/

create table Staging.Fact_VesselItinerary
	(
		VesselItineraryAlternateKey				int					not null,
		PostFixtureKey							int					not null,
		PortKey									int					not null,
		ETAStartDateKey							int					not null,
		ETAEndDateKey							int					not null,
		DateModifiedKey							int					not null,
		ChartererKey							int					not null,
		OwnerKey								int					not null,
		VesselKey								int					not null,
		COAKey									int					not null,
		DirectionKey							int					not null,
		ItineraryPortType						varchar(50)			null,			-- Degenerate Dimension Attributes
		Comments								varchar(500)		null,
		NORStartDate							date				null,
		ETAOriginalDate							date				null,
		ETAOriginalCreateDate					date				null,
		ETAEndOriginal							date				null,
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
		Direction								varchar(500)		null,
		ProductType								varchar(500)		null,
		VesselPortStatusOfficial				varchar(50)			null,
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
		NominatedQuantity						decimal(18, 5)		null,
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