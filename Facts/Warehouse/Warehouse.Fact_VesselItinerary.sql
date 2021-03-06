drop table if exists Warehouse.Fact_VesselItinerary;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	08/16/2019
Description:	Creates the Warehouse.Fact_VesselItinerary table
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
Brian Boswick	12/14/2020	Added EOSPStartDate to replace NORStartDate
Brian Boswick	04/29/2021	Added FirstLoadEventDateKey
Brian Boswick	07/12/2021	Removed COAKey
==========================================================================================================	
*/

create table Warehouse.Fact_VesselItinerary
	(
		VesselItineraryKey						int					not null identity(1, 1),
		VesselItineraryAlternateKey				int					not null,
		PostFixtureKey							int					not null,
		PortKey									int					not null,
		ETAStartDateKey							int					not null,		-- Degenerate Dimension Attributes
		ETAEndDateKey							int					not null,
		DateModifiedKey							int					not null,
		ChartererKey							int					not null,
		OwnerKey								int					not null,
		VesselKey								int					not null,
		DirectionKey							int					not null,
		FirstLoadEventDateKey					int					not null,
		ItineraryPortType						varchar(50)			null,
		Comments								varchar(5000)		null,
		NORStartDate							date				null,
		EOSPStartDate							date				null,
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
		RowCreatedDate							datetime			not null,
		RowUpdatedDate							datetime			not null,
		constraint [PK_Warehouse_Fact_VesselItinerary_Key] primary key clustered 
		(
			VesselItineraryKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];