drop table if exists Staging.Fact_Nomination;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	06/21/2021
Description:	Creates the Staging.Fact_Nomination table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create table Staging.Fact_Nomination
	(
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
		LoadPortAlternateKey					int					null,			-- ETL fields
		DischargePortAlternateKey				int					null,
		PostFixtureAlternateKey					int					null,		
		constraint [PK_Staging_Fact_Nomination_AltKey] primary key clustered 
		(
			ParcelAlternateKey, NominationAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];