/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/06/2019
Description:	Creates the LoadFact_FixtureBerth stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_FixtureBerth;
go

create procedure ETL.LoadFact_FixtureBerth
as
begin
	set nocount on;

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_FixtureBerth', 'U') is not null
		truncate table Staging.Fact_FixtureBerth;

	if object_id(N'Staging.Fact_FixtureBerthEvents', 'U') is not null
		truncate table Staging.Fact_FixtureBerthEvents;

	begin try
		-- Get Unique FixtureBerth records
		with LoadPortBerths	(
								PostFixtureAlternateKey,
								PortAlternateKey,
								BerthAlternateKey,
								LoadDischargeAlternateKey,
								ParcelAlternateKey,
								ProductAlternateKey,
								ParcelQuantity,
								LoadDischarge
							)
		as
		(
			select
				distinct
					p.RelatedSpiFixtureId,
					loadport.RelatedPortId,
					loadberth.RelatedBerthId,
					loadport.QBRecId,
					p.QbRecId,
					prod.QBRecId,
					p.BLQty,
					loadport.[Type]
				from
					Parcels p
						join ParcelPorts loadport
							on p.RelatedLoadPortID = loadport.QBRecId
						join ParcelBerths loadberth
							on p.RelatedLoadBerth = loadberth.QBRecId
						join ParcelProducts parprod
							on parprod.QBRecId = p.RelatedParcelProductId
						join Products prod
							on prod.QBRecId = parprod.RelatedProductId
				where
					p.RelatedSpiFixtureId is not null
		),
		DischPortBerths	(
							PostFixtureAlternateKey,
							PortAlternateKey,
							BerthAlternateKey,
							LoadDischargeAlternateKey,
							ParcelAlternateKey,
							ProductAlternateKey,
							ParcelQuantity,
							LoadDischarge
						)
		as
		(
			select
				distinct
					p.RelatedSpiFixtureId,
					dischport.RelatedPortId,
					dischberth.RelatedBerthId,
					dischport.QBRecId,
					p.QbRecId,
					prod.QBRecId,
					p.BLQty,
					dischport.[Type]
				from
					Parcels p
						join ParcelPorts dischport
							on p.RelatedDischPortId = dischport.QBRecId
						join ParcelBerths dischberth
							on p.RelatedDischBerth = dischberth.QBRecId
						join ParcelProducts parprod
							on parprod.QBRecId = p.RelatedParcelProductId
						join Products prod
							on prod.QBRecId = parprod.RelatedProductId
				where
					p.RelatedSpiFixtureId is not null
		),
		UniqueFixtureBerths	(
								PostFixtureAlternateKey,
								PortAlternateKey,
								BerthAlternateKey,
								LoadDischargeAlternateKey,
								ParcelAlternateKey,
								ProductAlternateKey,
								ParcelQuantity,
								LoadDischarge
							)
		as
		(
			select * from LoadPortBerths
			union all
			select * from DischPortBerths
		)


		insert
				Staging.Fact_FixtureBerth with (tablock)
											(
												PostFixtureAlternateKey,
												PortAlternateKey,
												BerthAlternateKey,
												LoadDischargeAlternateKey,
												ParcelAlternateKey,
												PortBerthKey,
												ProductKey,
												PostFixtureKey,
												VesselKey,
												FirstEventDateKey,
												ParcelKey,
												LoadDischarge,
												ParcelQuantity,
												RecordStatus
											)
			select
				distinct
					ufb.PostFixtureAlternateKey					PostFixtureAlternateKey,
					isnull(ufb.PortAlternateKey, -1)			PortAlternateKey,
					isnull(ufb.BerthAlternateKey, -1)			BerthAlternateKey,
					ufb.LoadDischargeAlternateKey				LoadDischargeAlternateKey,
					isnull(ufb.ParcelAlternateKey, -1)			ParcelAlternateKey,
					--isnull([port].PortKey, -1)					PortKey,
					--isnull(berth.BerthKey, -1)					BerthKey,
					isnull(portberth.PortBerthKey, -1)			PortBerthKey,
					isnull(product.ProductKey, -1)				ProductKey,
					isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
					isnull(vessel.VesselKey, -1)				VesselKey,
					-1											FirstEventDateKey,
					isnull(parcel.ParcelKey, -1)				ParcelKey,
					ufb.LoadDischarge							LoadDischarge,
					ufb.ParcelQuantity							ParcelQuantity,
					--isnull(rs.RecordStatus, @NewRecord)			RecordStatus,
					1
				from
					UniqueFixtureBerths ufb
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = ufb.PostFixtureAlternateKey
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join Warehouse.Dim_PortBerth portberth
							on portberth.PortAlternateKey = ufb.PortAlternateKey
								and portberth.BerthAlternateKey = ufb.BerthAlternateKey
						left join Warehouse.Dim_Product product
							on product.ProductAlternateKey = ufb.ProductAlternateKey
						left join Warehouse.Dim_Parcel parcel
							on parcel.ParcelAlternateKey = ufb.ParcelAlternateKey
						--left join Warehouse.Dim_Port [port]
						--	on [port].PortAlternateKey = parcel.RelatedPortId
						--left join Warehouse.Dim_Berth berth
						--	on berth.BerthAlternateKey = parcel.RelatedBerthId
						--left join	(
						--				select
						--						sum(qty.BLQty) TotalQuantity,
						--						qty.RelatedSpiFixtureId PostFixtureAlternateKey
						--					from
						--						Parcels qty
						--					group by
						--						qty.RelatedSpiFixtureId
						--			) totqty
						--	on totqty.PostFixtureAlternateKey = ufb.RelatedSpiFixtureId
						left join	(
										select
												@ExistingRecord RecordStatus,
												PostFixtureAlternateKey,
												PortAlternateKey,
												BerthAlternateKey,
												LoadDischargeAlternateKey,
												ParcelAlternateKey
											from
												Warehouse.Fact_FixtureBerth
									) rs
							on rs.PostFixtureAlternateKey = ufb.PostFixtureAlternateKey
								and rs.PortAlternateKey = ufb.PortAlternateKey
								and rs.BerthAlternateKey = ufb.BerthAlternateKey
								and rs.LoadDischargeAlternateKey = ufb.LoadDischargeAlternateKey
								and rs.ParcelAlternateKey = ufb.ParcelAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging FixtureBerth records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Update FirstEventDateKey
	begin try
		update
				Staging.Fact_FixtureBerth
			set
				FirstEventDateKey = wc.DateKey
			from
				(
					select
							min(convert(date, e.StartDate)) FirstEventDate,
							pb.RelatedSpiFixtureId PostFixtureAlternateKey
						from
							SOFEvents e
								join ParcelBerths pb 
									on e.RelatedParcelBerthId = pb.QBRecId
						group by
							pb.RelatedSpiFixtureId
				) fe
					join Warehouse.Dim_Calendar wc
						on wc.FullDate = fe.FirstEventDate
			where
				fe.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating FirstEventDateKey - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Get FixtureBerth events
	--begin try
	--	insert
	--			Staging.Fact_FixtureBerthEvents with (tablock)
	--		select
	--			distinct
	--				pb.RelatedSpiFixtureId							PostFixtureAlternateKey,
	--				pb.QBRecId										ParcelBerthAlternateKey,
	--				row_number() over	(
	--										partition by pb.QBRecId 
	--										order by try_convert(date, e.StartDate), try_convert(time, e.StartTime)
	--									)							EventNum,
	--				en.EventNameReports								EventName,
	--				lead(e.RelatedPortTimeEventId, 1, 0)  over	(
	--																partition by pb.QBRecId
	--																order by try_convert(date, e.StartDate), try_convert(time, e.StartTime)
	--															)	NextEventId,
	--				lead(en.EventNameReports, 1, 0)  over	(
	--															partition by pb.QBRecId
	--															order by try_convert(date, e.StartDate), try_convert(time, e.StartTime)
	--														)		NextEventName,
	--				try_convert(date, e.StartDate)					StartDate,
	--				try_convert(time, e.StartTime)					StartTime,
	--				--null											StartDateTime,
	--				datetimefromparts	(
	--										year(try_convert(date, e.StartDate)),
	--										month(try_convert(date, e.StartDate)),
	--										day(try_convert(date, e.StartDate)),
	--										datepart(hour, try_convert(time, e.StartTime)),
	--										datepart(minute, try_convert(time, e.StartTime)),
	--										0,
	--										0
	--									)							StartDateTime,
	--				try_convert(date, e.StopDate)					StopDate,
	--				try_convert(time, e.StopTime)					StopTime,
	--				--null											StopDateTime,
	--				datetimefromparts	(
	--										year(try_convert(date, e.StopDate)),
	--										month(try_convert(date, e.StopDate)),
	--										day(try_convert(date, e.StopDate)),
	--										datepart(hour, try_convert(time, e.StopTime)),
	--										datepart(minute, try_convert(time, e.StopTime)),
	--										0,
	--										0
	--									)							StopDateTime,
	--				null											Duration
	--			from
	--				SOFEvents e
	--					join PortEventTimes en
	--						on e.RelatedPortTimeEventId = en.QBRecId
	--					join ParcelBerths pb
	--						on pb.QBRecId = e.RelatedParcelBerthId
	--					join Warehouse.Dim_Calendar sd
	--						on sd.FullDate = convert(date, e.StartDate)
	--					join Warehouse.Dim_Calendar ed
	--						on sd.FullDate = convert(date, e.StopDate)
	--			where
	--				pb.RelatedSpiFixtureId is not null;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Staging FixtureBerth events - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	-- Calculate Duration
	--begin try
	--	-- Create full start and stop datetimes
	--	update
	--			Staging.Fact_FixtureBerthEvents
	--		set
	--			StartDateTime = datetimefromparts(year(StartDate), month(StartDate), day(StartDate), datepart(hour, StartTime), datepart(minute, StartTime), 0, 0),
	--			StopDateTime = datetimefromparts(year(StopDate), month(StopDate), day(StopDate), datepart(hour, StopTime), datepart(minute, StopTime), 0, 0);
	--end try
	--begin catch
	--	select @ErrorMsg = 'Calculating duration of events - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	-- Update WaitTimeNOR_Berth
	begin try
		select 1
		--update
		--		Staging.Fact_FixtureBerth
		--	set
		--		ProductKey = 0
		--	from
		--		(
		--			select
		--					sum(convert(decimal(18, 4), e.dur)) FirstEventDate,
		--					pb.RelatedSpiFixtureId PostFixtureAlternateKey
		--				from
		--					SOFEvents e
		--						join ParcelBerths pb 
		--							on e.RelatedParcelBerthId = pb.QBRecId
		--				group by
		--					pb.RelatedSpiFixtureId
		--		) fe
		--	where
		--		pp.QBRecId = Staging.Fact_FixtureBerth.ParcelProductID;
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeNOR_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

/*	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_SOFEvent
			select
					evt.EventAlternateKey,
					evt.PortKey,
					evt.BerthKey,
					evt.StartDateKey,
					evt.StopDateKey,
					evt.ProductKey,
					evt.PostFixtureKey,
					evt.VesselKey,
					evt.ParcelKey,
					evt.LoadPortBerthKey,
					evt.DischargePortBerthKey,
					evt.ProrationType,
					evt.EventType,
					evt.IsLaytime,
					evt.IsPumpingTime,
					evt.LoadDischarge,
					evt.Comments,
					evt.ParcelNumber,
					evt.StartDateTime,
					evt.StopDateTime,
					evt.Duration,
					evt.LaytimeActual,
					evt.LaytimeAllowed,
					evt.LaytimeAllowedProrated,
					evt.ParcelQuantity,
					getdate() RowStartDate,
					getdate() RowUpdatedDate
				from
					Staging.Fact_SOFEvent evt
				where
					evt.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch */
end