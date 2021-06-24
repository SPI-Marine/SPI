drop table if exists Warehouse.Fact_Nomination;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	06/23/2021
Description:	Creates the Warehouse.Fact_Nomination table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create table Warehouse.Fact_Nomination
	(
		NominationKey							int					not null identity(1, 1),
		ParcelAlternateKey						int					not null,
		NominationAlternateKey					int					not null,
		PostFixtureKey							int					not null,
		LoadPortKey								int					not null,
		DischargePortKey						int					not null,
		LoadBerthKey							int					not null,
		DischargeBerthKey						int					not null,
		LoadPortBerthKey						int					not null,
		DischargePortBerthKey					int					not null,
		ProductKey								int					not null,
		BillLadingDateKey						int					not null,
		DimParcelKey							int					not null,
		VesselKey								int					not null,
		COAKey									int					not null,
		NominatedQty							decimal(18, 6)		null,			-- Metrics
		BLQty									decimal(18, 6)		null,
		TentCargoNomOriginalQty					decimal(18, 6)		null,
		LoadNORStartDate						date				null,			-- Degenerate Dimension Attributes
		LoadLastHoseOffDate						date				null,			
		DischargeNORStartDate					date				null,
		DischargeLastHoseOffDate				date				null,
		TentCargoNomDateOriginal				date				null,
		[Status]								varchar(250)		null,
		RowCreatedDate							datetime			not null,
		constraint [PK_Warehouse_Fact_Nomination_Key] primary key clustered 
		(
			NominationKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];