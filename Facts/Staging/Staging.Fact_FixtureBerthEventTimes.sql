/*
==========================================================================================================
Author:			Brian Boswick
Create date:	06/06/2019
Description:	Creates the Staging.Fact_FixtureBerthEventTimes table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_FixtureBerthEventTimes;
go

create table Staging.Fact_FixtureBerthEventTimes
	(
		PostFixtureAlternateKey						int					not null,
		PortBerthKey								int					not null,
		LoadDischarge								varchar(50)			not null,		-- Degenerate Dimension Attributes
		ProductType									nvarchar(100)		not null,
		ParcelQuantityTShirtSize					varchar(50)			not null,
		AverageWaitTimeNOR_Berth					decimal(20, 8)		null,		-- Metrics
		AverageWaitTimeBerth_HoseOn					decimal(20, 8)		null,
		AverageWaitTimeHoseOn_CommenceLoad			decimal(20, 8)		null,
		AverageWaitTimeHoseOn_CommenceDischarge		decimal(20, 8)		null,
		AverageWaitTimeBerth_HoseOff				decimal(20, 8)		null,
		AverageWaitTimeCompleteLoad_HoseOff			decimal(20, 8)		null,
		AverageWaitTimeCompleteDischarge_HoseOff	decimal(20, 8)		null,
		AverageLayTimeNOR_Berth						decimal(20, 8)		null,
		AverageLayTimeBerth_HoseOn					decimal(20, 8)		null,
		AverageLayTimeHoseOn_CommenceLoad			decimal(20, 8)		null,
		AverageLayTimeHoseOn_CommenceDischarge		decimal(20, 8)		null,
		AverageLayTimeBerth_HoseOff					decimal(20, 8)		null,
		AverageLayTimeCompleteLoad_HoseOff			decimal(20, 8)		null,
		AverageLayTimeCompleteDischarge_HoseOff		decimal(20, 8)		null,
		AverageLayTimePumpingTime					decimal(20, 8)		null,
		AverageLayTimePumpingRate					decimal(20, 8)		null,
		AverageLaytimeActual						decimal(20, 8)		null,
		AverageLaytimeAllowed						decimal(20, 8)		null,
		AveragePumpTime								decimal(20, 8)		null,
		constraint [PK_Staging_Fact_FixtureBerthEventTimes_AltKeys] primary key clustered 
		(
			PostFixtureAlternateKey,
			PortBerthKey,
			LoadDischarge,
			ProductType,
			ParcelQuantityTShirtSize asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];