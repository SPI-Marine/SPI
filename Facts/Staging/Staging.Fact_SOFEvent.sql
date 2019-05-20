/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Staging.Fact_SOFEvent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_SOFEvent;
go

create table Staging.Fact_SOFEvent
	(
		EventAlternateKey		int					not null,
		ParcelAlternateKey		int					not null,
		ParcelPortAlternateKey	int					not null,
		PortKey					int					not null,
		BerthKey				int					not null,
		StartDateKey			int					not null,
		StopDateKey				int					not null,
		ProductKey				int					not null,
		PostFixtureKey			int					not null,
		VesselKey				int					not null,
		ParcelKey				int					not null,
		PortBerthKey			int					not null,
		ProrationType			nvarchar(100)		null,		-- Degenerate Dimension Attributes
		EventType				nvarchar(250)		null,
		IsLaytime				char(1)				null,
		IsPumpingTime			char(1)				null,
		LoadDischarge			nvarchar(100)		null,
		Comments				nvarchar(1000)		null,
		ParcelNumber			smallint			null,
		StartDateTime			varchar(50)			null,
		StopDateTime			varchar(50)			null,
		StartDateTimeSort		datetime			null,
		Duration				decimal(18, 4)		null,		-- Metrics
		LaytimeActual			decimal(18, 4)		null,
		LaytimeAllowed			decimal(18, 4)		null,
		LaytimeAllowedProrated	decimal(18, 4)		null,
		ProrationPercentage		decimal(18, 4)		null,		-- ETL fields
		ParcelQuantity			decimal(18, 4)		null,
		ParcelQuantityETL		decimal(18, 4)		null,
		TotalQuantity			decimal(18, 4)		null,
		StartTime				time				null,
		StopTime				time				null,
		StartDate				datetime			null,
		StopDate				datetime			null,
		ParcelProductID			int					null,
		constraint [PK_Staging_Fact_SOFEvent_QBRecId] primary key clustered 
		(
			EventAlternateKey, ParcelAlternateKey, ParcelPortAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];