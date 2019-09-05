/*
==========================================================================================================
Author:			Brian Boswick
Create date:	08/16/2019
Description:	Creates the Staging.Fact_VesselItinerary table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_VesselItinerary;
go

create table Staging.Fact_VesselItinerary
	(
		VesselItineraryAlternateKey				int					not null,
		PostFixtureKey							int					not null,
		PortKey									int					not null,
		ETAStartDateKey							int					null,		-- Degenerate Dimension Attributes
		ETAEndDateKey							int					null,
		ItineraryPortType						varchar(50)			null,
		Comments								varchar(500)		null,
		RelatedParcelPortID						int					null,	-- ETL fields
		RelatedPortID							int					null,
		constraint [PK_Staging_Fact_VesselItinerary_AltKey] primary key clustered 
		(
			VesselItineraryAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];