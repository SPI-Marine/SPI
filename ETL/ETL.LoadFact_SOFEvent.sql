/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the LoadFact_SOFEvent stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	03/15/2019	Added LoadPortBerthKey and DischargePortBerthKey
Brian Boswick	05/20/2019	Remove deleted records from Warehouse
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_SOFEvent;
go

create procedure ETL.LoadFact_SOFEvent
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_SOFEvent', 'U') is not null
		truncate table Staging.Fact_SOFEvent;

	begin try
		insert
				Staging.Fact_SOFEvent with (tablock)
		select
			distinct
				sof.QBRecId									EventAlternateKey,
				parcel.ParcelId								ParcelAlternateKey,
				isnull(parcel.ParcelPortAlternateKey, -1)	ParcelPortAlternateKey,
				isnull([port].PortKey, -1)					PortKey,
				isnull(berth.BerthKey, -1)					BerthKey,
				isnull(startdate.DateKey, 18991230)			StartDateKey,
				isnull(stopdate.DateKey, 47001231)			StopDateKey,
				-1											ProductKey,
				isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
				isnull(vessel.VesselKey, -1)				VesselKey,
				isnull(wparcel.ParcelKey, -1)				ParcelKey,
				isnull(portberth.PortBerthKey, -1)			PortBerthKey,
				sof.LaytimeProationType						ProrationType,
				eventtype.EventNameReports					EventType,
				case sof.Laytime
					when 1
						then 'Y'
					else 'N'
				end											IsLaytime,
				case sof.PumpingTime
					when 1
						then 'Y'
					else 'N'
				end											IsPumpingTime,
				parcel.LoadDischarge						LoadDischarge,
				sof.Comments								Comments,
				null										ParcelNumber,
				concat	(
							convert(varchar(50), try_convert(datetime, sof.StartDate), 103),
							' ',
							left(convert(varchar(50), try_convert(time, sof.StartTime), 108), 5)
						)									StartDateTime,
				concat	(
							convert(varchar(50), try_convert(datetime, sof.StopDate), 103),
							' ',
							left(convert(varchar(50), try_convert(time, sof.StopTime), 108), 5)
						)									StopDateTime,
				try_convert	(
								datetime,
								sof.StartDate
								+ ' '
								+ sof.StartTime
							)								StartDateTimeSort,
				null										Duration,
				null										LaytimeActual,
				case
					when epostfixture.Disch_FAC = 1 and parcel.LoadDischarge = 'Discharge'
						then 0
					else parcel.LaytimeAllowed
				end											LaytimeAllowed,
				null										LaytimeAllowedProrated,
				null										ProrationPercentage,
				case
					when parcel.RelatedPortId = parcel.LoadPortAlternateKey
							and parcel.RelatedBerthId = parcel.LoadBerthAlternateKey
							and parcel.LoadDischarge = 'Load'
						then parcel.ParcelQuantity
					when parcel.RelatedPortId = parcel.DischPortAlternateKey
							and parcel.RelatedBerthId = parcel.DischBerthAlternateKey
							and parcel.LoadDischarge = 'Discharge'
						then parcel.ParcelQuantity
					else null
				end											ParcelQuantity,
				parcel.ParcelQuantity						ParcelQuantityETL,
				totqty.TotalQuantity						TotalQuantity,
				try_convert(time, sof.StartTime)			StartTime,
				try_convert(time, sof.StopTime)				StopTime,
				try_convert(datetime, sof.StartDate)		StartDate,
				try_convert(datetime, sof.StopDate)			StopDate,
				parcel.ParcelProductId						ParcelProductId
			from
				SOFEvents sof
					left join Warehouse.Dim_Calendar startdate
						on try_convert(date, sof.StartDate) = startdate.FullDate
					left join Warehouse.Dim_Calendar stopdate
						on try_convert(date, sof.StopDate) = stopdate.FullDate
					left join PortEventTimes eventtype
						on sof.RelatedPortTimeEventId = eventtype.QBRecId
					join	(
								select
									distinct
										pb.QBRecId						ParcelBerthId,
										p.QbRecId						ParcelId,
										pb.RelatedSpiFixtureId			PostFixtureAlternateKey,
										pb.RelatedLDPId					ParcelPortAlternateKey,
										eventport.RelatedPortId			RelatedPortId,
										pb.RelatedBerthId				RelatedBerthId,
										pb.LaytimeAllowedBerthHrs_QBC	LaytimeAllowed,
										p.BLQty							ParcelQuantity,
										p.RelatedParcelProductId		ParcelProductId,
										p.RelatedLoadPortID				LoadPortID,
										p.RelatedLoadBerth				LoadBerthID,
										p.RelatedDischPortId			DischargePortID,
										p.RelatedDischBerth				DischargeBerthID,
										loadport.RelatedPortId			LoadPortAlternateKey,
										loadberth.[RelatedBerthId]		LoadBerthAlternateKey,
										loaddischarge.[Type]			LoadDischarge,
										dischport.[RelatedPortId]		DischPortAlternateKey,
										dischberth.RelatedBerthId		DischBerthAlternateKey										
									from
										ParcelBerths pb
											join ParcelPorts eventport
												on eventport.QBRecId = pb.RelatedLDPId
											join Parcels p
												on pb.RelatedSpiFixtureId = p.RelatedSpiFixtureId
											join ParcelPorts loadport
												on loadport.QBRecId = p.RelatedLoadPortID
											join ParcelBerths loadberth
												on loadberth.QBRecId = p.RelatedLoadBerth
											join ParcelPorts dischport
												on dischport.QBRecId = p.RelatedDischPortId
											join ParcelBerths dischberth
												on dischberth.QBRecId = p.RelatedDischBerth
											join ParcelPorts loaddischarge
												on pb.RelatedLDPId = loaddischarge.QBRecId
							) parcel
						on sof.RelatedParcelBerthId = parcel.ParcelBerthId
					left join Warehouse.Dim_Parcel wparcel
						on wparcel.ParcelAlternateKey = parcel.ParcelId
					left join Warehouse.Dim_PostFixture wpostfixture
						on wpostfixture.PostFixtureAlternateKey = parcel.PostFixtureAlternateKey
					left join PostFixtures epostfixture
						on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Vessel vessel
						on vessel.VesselAlternateKey = epostfixture.RelatedVessel
					left join Warehouse.Dim_Port [port]
						on [port].PortAlternateKey = parcel.RelatedPortId
					left join Warehouse.Dim_Berth berth
						on berth.BerthAlternateKey = parcel.RelatedBerthId
					left join Warehouse.Dim_PortBerth portberth
						on portberth.PortAlternateKey = parcel.RelatedPortId
							and portberth.BerthAlternateKey = parcel.RelatedBerthId
					left join	(
									select
											sum(qty.BLQty) TotalQuantity,
											qty.RelatedSpiFixtureId PostFixtureAlternateKey
										from
											Parcels qty
										group by
											qty.RelatedSpiFixtureId
								) totqty
						on totqty.PostFixtureAlternateKey = parcel.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging SOFEvent records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Update ProrationPercentage
	begin try
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				ProrationPercentage =	case	
											when isnull(TotalQuantity, 0) <> 0
												then ParcelQuantityETL/TotalQuantity
											else null
										end;
	end try
	begin catch
		select @ErrorMsg = 'Updating ProrationPercentage - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update event duration
	begin try
		-- Create full start and stop datetimes
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				StartDate = datetimefromparts(year(StartDate), month(StartDate), day(StartDate), datepart(hour, StartTime), datepart(minute, StartTime), 0, 0),
				StopDate = datetimefromparts(year(StopDate), month(StopDate), day(StopDate), datepart(hour, StopTime), datepart(minute, StopTime), 0, 0);
	
		-- Calculate Duration
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				Duration =	case
								when StartDateKey > 19000000 and StopDateKey < 47000000
									then datediff(minute, StartDate, StopDate)/60.0
								else null
							end;

		-- Calculate LaytimeActual
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				LaytimeActual =	ee.LtUsedProrationAmtHrs_QBC
			from
				SOFEvents ee
					join Staging.Fact_SOFEvent fe
						on ee.QBRecId = fe.EventAlternateKey;

		-- Calculate LaytimeAllowedProrated
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				LaytimeAllowedProrated = ProrationPercentage*LaytimeAllowed;
	end try
	begin catch
		select @ErrorMsg = 'Updating SOFEvent Duration/LaytimeActual/LaytimeAllowedProrated - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update ParcelNumber
	begin try
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				ParcelNumber = parcelnumbers.ParcelNumber
			from
				(
					select
							row_number() over (partition by p.RelatedSpiFixtureId order by p.QbRecId)	ParcelNumber,
							p.RelatedSpiFixtureId,
							p.QbRecId ParcelId
						from
							Parcels p
				) parcelnumbers
			where
				parcelnumbers.ParcelId = Staging.Fact_SOFEvent.ParcelAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating ParcelNumber - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update ProductKey
	begin try
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				ProductKey = wproduct.ProductKey
			from
				ParcelProducts pp
					join Warehouse.Dim_Product wproduct
						on pp.RelatedProductId = wproduct.ProductAlternateKey
			where
				pp.QBRecId = Staging.Fact_SOFEvent.ParcelProductID;
	end try
	begin catch
		select @ErrorMsg = 'Updating ProductKey - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_SOFEvent', 'U') is not null
		truncate table Warehouse.Fact_SOFEvent;

	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_SOFEvent with (tablock)
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
					evt.PortBerthKey,
					evt.ProrationType,
					evt.EventType,
					evt.IsLaytime,
					evt.IsPumpingTime,
					evt.LoadDischarge,
					evt.Comments,
					evt.ParcelNumber,
					evt.StartDateTime,
					evt.StopDateTime,
					evt.StartDateTimeSort,
					evt.Duration,
					evt.LaytimeActual,
					evt.LaytimeAllowed,
					evt.LaytimeAllowedProrated,
					evt.ParcelQuantity,
					getdate() RowStartDate
				from
					Staging.Fact_SOFEvent evt;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Remove deleted source records
	--begin try
	--	delete
	--			Warehouse.Fact_SOFEvent with (tablock)
	--		where
	--			not exists	(
	--							select
	--									1
	--								from
	--									SOFEvents se
	--								where
	--									se.QBRecId = Warehouse.Fact_SOFEvent.EventAlternateKey
	--						);
	--end try
	--begin catch
	--	select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch
end