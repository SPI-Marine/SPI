/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Staging.Fact_SOFEvent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey
Brian Boswick	02/14/2020	Renamed multiple metrics
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	03/25/2021	Removed fields to refactor to remove Parcel/Product grain and change to 
							event level grain
==========================================================================================================	
*/

drop table if exists Staging.Fact_SOFEvent;
go

create table Staging.Fact_SOFEvent
	(
		EventAlternateKey		int					not null,
		ParcelPortAlternateKey	int					not null,
		PortKey					int					not null,
		BerthKey				int					not null,
		StartDateKey			int					not null,
		StopDateKey				int					not null,
		PostFixtureKey			int					not null,
		VesselKey				int					not null,
		PortBerthKey			int					not null,
		ChartererKey			int					not null,
		OwnerKey				int					not null,
		COAKey					int					not null,
		ProrationType			nvarchar(100)		null,		-- Degenerate Dimension Attributes
		EventType				nvarchar(250)		null,
		IsLaytime				char(1)				null,
		IsPumpingTime			char(1)				null,
		LoadDischarge			nvarchar(100)		null,
		Comments				nvarchar(1000)		null,
		StartDateTime			varchar(50)			null,
		StopDateTime			varchar(50)			null,
		StartDateTimeSort		datetime			null,
		Duration				decimal(18, 5)		null,		-- Metrics
		LaytimeUsed				decimal(18, 5)		null,
		LaytimeAllowed			decimal(18, 5)		null,
		ProrationPercentage		decimal(18, 5)		null,		-- ETL fields
		TotalQuantity			decimal(18, 5)		null,
		StartTime				time				null,
		StopTime				time				null,
		StartDate				datetime			null,
		StopDate				datetime			null,
		constraint [PK_Staging_Fact_SOFEvent_QBRecId] primary key clustered 
		(
			EventAlternateKey, ParcelPortAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];