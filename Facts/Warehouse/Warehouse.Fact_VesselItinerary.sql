/*
==========================================================================================================
Author:			Brian Boswick
Create date:	08/16/2019
Description:	Creates the Warehouse.Fact_VesselItinerary table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	10/10/2019	Added DateModifiedKey column
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_VesselItinerary;
go

create table Warehouse.Fact_VesselItinerary
	(
		VesselItineraryKey						int					not null identity(1, 1),
		VesselItineraryAlternateKey				int					not null,
		PostFixtureKey							int					not null,
		PortKey									int					not null,
		ETAStartDateKey							int					not null,		-- Degenerate Dimension Attributes
		ETAEndDateKey							int					not null,
		DateModifiedKey							int					not null,
		ItineraryPortType						varchar(50)			null,
		Comments								varchar(500)		null,
		RowCreatedDate							date				not null,
		constraint [PK_Warehouse_Fact_VesselItinerary_Key] primary key clustered 
		(
			VesselItineraryKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];