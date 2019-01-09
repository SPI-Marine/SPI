/*
==========================================================================================================
Author:			Brian Boswick
Create date:	01/08/2019
Description:	Creates the LoadFact_ParcelFinance stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_ParcelFinance;
go

create procedure ETL.LoadFact_ParcelFinance
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_ParcelFinance', 'U') is not null
		truncate table Staging.Fact_ParcelFinance;

	begin try
		insert
				Staging.Fact_ParcelFinance
		select
			distinct
				charge.RelatedSPIFixtureId	PostFixtureAlternateKey,
				-1	ParcelAlternateKey,
				-1	PortKey,
				-1	BerthKey,
				-1	ProductKey,
				isnull(wpostfixture.PostFixtureKey, -1),
				isnull(vessel.VesselKey, -1),
				charge.[Type]	ChargeType,
				charge.[Description] ChargeDescription,
				null	ParcelNumber,
				charge.Amount	Charge,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				AdditionalCharges charge
					left join Warehouse.Dim_PostFixture wpostfixture
						on wpostfixture.PostFixtureAlternateKey = charge.RelatedSPIFixtureId
					left join PostFixtures epostfixture
						on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Vessel vessel
						on vessel.VesselAlternateKey = epostfixture.RelatedVessel
					left join	(
									select
											@ExistingRecord RecordStatus,
											PostFixtureAlternateKey,
											ParcelAlternateKey
										from
											Warehouse.Fact_ParcelFinance
								) rs
						on rs.PostFixtureAlternateKey = charge.RelatedSPIFixtureId
							and rs.ParcelAlternateKey = -1;
	end try
	begin catch
		select @ErrorMsg = 'Staging ParcelFinance records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	---- Update LoadDischarge
	--begin try
	--	update
	--			Staging.Fact_ParcelFinance
	--		set
	--			LoadDischarge = pp.[Type]
	--		from
	--			ParcelPorts pp
	--		where
	--			pp.QBRecId = Staging.Fact_ParcelFinance.ParcelPortAlternateKey;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Updating LoadDischarge - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	---- Update ProrationPercentage
	--begin try
	--	update
	--			Staging.Fact_ParcelFinance
	--		set
	--			ProrationPercentage = ParcelQuantity/TotalQuantity;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Updating ProrationPercentage - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	---- Update event duration
	--begin try
	--	-- Create full start and stop datetimes
	--	update
	--			Staging.Fact_ParcelFinance
	--		set
	--			StartDate = datetimefromparts(year(StartDate), month(StartDate), day(StartDate), datepart(hour, StartTime), datepart(minute, StartTime), 0, 0),
	--			StopDate = datetimefromparts(year(StopDate), month(StopDate), day(StopDate), datepart(hour, StopTime), datepart(minute, StopTime), 0, 0);
	
	--	-- Calculate Duration
	--	update
	--			Staging.Fact_ParcelFinance
	--		set
	--			Duration =	case
	--							when StartDateKey > 19000000 and StopDateKey < 47000000
	--								then datediff(minute, StartDate, StopDate)/60.0
	--							else null
	--						end;

	--	-- Calculate LaytimeActual
	--	update
	--			Staging.Fact_ParcelFinance
	--		set
	--			LaytimeActual =	case IsLaytime
	--								when 'Y'
	--									then ProrationPercentage*Duration
	--								else null
	--							end;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Updating ParcelFinance Duration/LaytimeActual - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	---- Update ParcelNumber
	--begin try
	--	update
	--			Staging.Fact_ParcelFinance
	--		set
	--			ParcelNumber = parcelnumbers.ParcelNumber
	--		from
	--			(
	--				select
	--						row_number() over (partition by p.RelatedSpiFixtureId order by p.QbRecId)	ParcelNumber,
	--						p.RelatedSpiFixtureId,
	--						p.QbRecId ParcelId
	--					from
	--						Parcels p
	--			) parcelnumbers
	--		where
	--			parcelnumbers.ParcelId = Staging.Fact_ParcelFinance.ParcelAlternateKey;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Updating ParcelNumber - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	---- Insert new events into Warehouse table
	--begin try
	--	insert
	--			Warehouse.Fact_ParcelFinance
	--		select
	--				evt.EventAlternateKey,
	--				evt.PortKey,
	--				evt.BerthKey,
	--				evt.StartDateKey,
	--				evt.StopDateKey,
	--				evt.ProductKey,
	--				evt.PostFixtureKey,
	--				evt.VesselKey,
	--				evt.ProrationType,
	--				evt.EventType,
	--				evt.IsLaytime,
	--				evt.IsPumpingTime,
	--				evt.LoadDischarge,
	--				evt.Comments,
	--				evt.ParcelNumber,
	--				evt.Duration,
	--				evt.LaytimeActual,
	--				evt.LaytimeAllowed,
	--				getdate() RowStartDate,
	--				getdate() RowUpdatedDate
	--			from
	--				Staging.Fact_ParcelFinance evt
	--			where
	--				evt.RecordStatus & @NewRecord = @NewRecord;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Loading Warehouse - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch
end