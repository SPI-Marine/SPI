/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Warehouse.Fact_SOFEvent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
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
		EndDateKey				int					not null,
		ProductKey				int					not null,
		PostFixtureKey			int					not null,
		VesselKey				int					not null,
		ProrationType			nvarchar(100)		null,		-- Degenerate Dimension Attributes
		EventType				nvarchar(250)		null,
		IsLaytime				char(1)				null,
		IsPumpingTime			char(1)				null,
		LoadDischarge			nvarchar(100)		null,
		Comments				nvarchar(1000)		null,
		Duration				decimal(18, 4)		null,		-- Metrics
		LaytimeActual			decimal(18, 4)		null,
		LaytimeAllowed			decimal(18, 4)		null,
		RecordStatus			int					not null,
		constraint [PK_Warehouse_Fact_SOFEvent_QBRecId] primary key clustered 
		(
			EventKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];