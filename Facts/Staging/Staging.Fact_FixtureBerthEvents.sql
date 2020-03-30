/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/08/2019
Description:	Creates the Staging.Fact_FixtureBerthEvents table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_FixtureBerthEvents;
go

create table Staging.Fact_FixtureBerthEvents
	(
		PostFixtureAlternateKey				int					not null,
		ParcelBerthAlternateKey				int					not null,
		LoadDischarge						varchar(50)			null,
		EventNum							smallint			not null,
		EventTypeId							int					null,
		EventName							varchar(50)			null,
		NextEventTypeId						int					null,
		NextEventName						varchar(50)			null,
		StartDateTime						datetime			null,
		Duration							decimal(20, 8)		null,
		LayTimeUsedProrated					decimal(20, 8)		null,
		IsPumpingTime						char(1)				null,
		IsLayTime							char(1)				null,
		constraint [PK_Staging_Fact_FixtureBerthEvents_AltKeys] primary key clustered 
		(
			PostFixtureAlternateKey,
			ParcelBerthAlternateKey,
			EventNum asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];