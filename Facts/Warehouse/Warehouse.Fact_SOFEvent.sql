/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Warehouse.Fact_SOFEvent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey
Brian Boswick	02/14/2020	Renamed multiple metrics
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_SOFEvent;
go

create table Warehouse.Fact_SOFEvent
	(
		EventKey				int					not null identity(1, 1),
		EventAlternateKey		int					not null,
		PortKey					int					not null,
		BerthKey				int					not null,
		StartDateKey			int					not null,
		StopDateKey				int					not null,
		ProductKey				int					not null,
		PostFixtureKey			int					not null,
		VesselKey				int					not null,
		ParcelKey				int					not null,
		PortBerthKey			int					not null,
		ChartererKey			int					not null,
		OwnerKey				int					not null,
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
		Duration				decimal(18, 5)		null,		-- Metrics
		LaytimeUsed				decimal(18, 5)		null,
		LaytimeAllowed			decimal(18, 5)		null,
		LaytimeAllowedProrated	decimal(18, 5)		null,
		ParcelQuantity			decimal(18, 5)		null,
		RowCreatedDate			datetime			not null,
		constraint [PK_Warehouse_Fact_SOFEvent_QBRecId] primary key clustered 
		(
			EventKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];