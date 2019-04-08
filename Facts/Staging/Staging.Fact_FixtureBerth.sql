/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/06/2019
Description:	Creates the Staging.Fact_FixtureBerth table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_FixtureBerth;
go

create table Staging.Fact_FixtureBerth
	(
		PostFixtureAlternateKey				int					not null,
		BerthAlternateKey					int					not null,
		LoadDischargeAlternateKey			int					not null,
		ParcelAlternateKey					int					not null,
		--PortKey								int					not null,
		--BerthKey							int					not null,
		PortBerthKey						int					not null,
		ProductKey							int					not null,
		PostFixtureKey						int					not null,
		VesselKey							int					not null,
		FirstEventDateKey					int					not null,
		ParcelKey							int					not null,
		LoadDischarge						varchar(50)			null,		-- Degenerate Dimension Attributes
		ParcelQuantityTShirtSize			varchar(50)			null,
		WaitTimeNOR_Berth					decimal(18, 4)		null,		-- Metrics
		WaitTimeBerth_HoseOn				decimal(20, 8)		null,
		WaitTimeBerth_HoseOff				decimal(20, 8)		null,
		WaitTimeCompleteLoad_HoseOff		decimal(20, 8)		null,
		WaitTimeCompleteDischarge_HoseOff	decimal(20, 8)		null,
		LayTimeNOR_Berth					decimal(18, 4)		null,
		LayTimeBerth_HoseOn					decimal(20, 8)		null,
		LayTimeBerth_HoseOff				decimal(20, 8)		null,
		LayTimeCompleteLoad_HoseOff			decimal(20, 8)		null,
		LayTimeCompleteDischarge_HoseOff	decimal(20, 8)		null,
		ParcelQuantity						decimal(18, 4)		null,
		LaytimeActual						decimal(18, 4)		null,
		LaytimeAllowed						decimal(18, 4)		null,
		Pumptime							decimal(18, 4)		null,
		RecordStatus						int					not null,
		constraint [PK_Staging_Fact_FixtureBerth_QBRecId] primary key clustered 
		(
			PostFixtureAlternateKey,
			BerthAlternateKey,
			LoadDischargeAlternateKey,
			ParcelAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];