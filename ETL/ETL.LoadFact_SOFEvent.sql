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
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey ETL logic
Brian Boswick	02/14/2020	Renamed multiple metrics
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
				isnull(wch.ChartererKey, -1)				ChartererKey,
				isnull(wo.OwnerKey, -1)						OwnerKey,
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
				null										LaytimeUsed,
				case
					when epostfixture.DischFAC = 1 and parcel.LoadDischarge = 'Discharge'
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
				SOFEvents sof with (nolock)
					left join Warehouse.Dim_Calendar startdate with (nolock)
						on try_convert(date, sof.StartDate) = startdate.FullDate
					left join Warehouse.Dim_Calendar stopdate with (nolock)
						on try_convert(date, sof.StopDate) = stopdate.FullDate
					left join PortEventTimes eventtype with (nolock)
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
										ParcelBerths pb with (nolock)
											join ParcelPorts eventport with (nolock)
												on eventport.QBRecId = pb.RelatedLDPId
											join Parcels p with (nolock)
												on pb.RelatedSpiFixtureId = p.RelatedSpiFixtureId
											join ParcelPorts loadport with (nolock)
												on loadport.QBRecId = p.RelatedLoadPortID
											join ParcelBerths loadberth with (nolock)
												on loadberth.QBRecId = p.RelatedLoadBerth
											join ParcelPorts dischport with (nolock)
												on dischport.QBRecId = p.RelatedDischPortId
											join ParcelBerths dischberth with (nolock)
												on dischberth.QBRecId = p.RelatedDischBerth
											join ParcelPorts loaddischarge with (nolock)
												on pb.RelatedLDPId = loaddischarge.QBRecId
							) parcel
						on sof.RelatedParcelBerthId = parcel.ParcelBerthId
					left join Warehouse.Dim_Parcel wparcel with (nolock)
						on wparcel.ParcelAlternateKey = parcel.ParcelId
					left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
						on wpostfixture.PostFixtureAlternateKey = parcel.PostFixtureAlternateKey
					left join PostFixtures epostfixture with (nolock)
						on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
					left join FullStyles fs with (nolock)
						on epostfixture.RelatedChartererFullStyle = fs.QBRecId
					left join Warehouse.Dim_Owner wo with (nolock)
						on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
					left join Warehouse.Dim_Charterer wch with (nolock)
						on wch.ChartererAlternateKey = fs.RelatedChartererParentID
					left join Warehouse.Dim_Vessel vessel with (nolock)
						on vessel.VesselAlternateKey = epostfixture.RelatedVessel
					left join Warehouse.Dim_Port [port] with (nolock)
						on [port].PortAlternateKey = parcel.RelatedPortId
					left join Warehouse.Dim_Berth berth with (nolock)
						on berth.BerthAlternateKey = parcel.RelatedBerthId
					left join Warehouse.Dim_PortBerth portberth with (nolock)
						on portberth.PortAlternateKey = parcel.RelatedPortId
							and portberth.BerthAlternateKey = parcel.RelatedBerthId
					left join	(
									select
											sum(qty.BLQty) TotalQuantity,
											qty.RelatedSpiFixtureId PostFixtureAlternateKey
										from
											Parcels qty with (nolock)
										group by
											qty.RelatedSpiFixtureId
								) totqty
						on totqty.PostFixtureAlternateKey = parcel.PostFixtureAlternateKey
			where
				sof.StartDate is not null;
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

		-- Calculate LaytimeUsed
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				LaytimeUsed =	ee.LtUsedProrationAmtHrs_QBC
			from
				SOFEvents ee with (nolock)
					join Staging.Fact_SOFEvent fe
						on ee.QBRecId = fe.EventAlternateKey;

		-- Calculate LaytimeAllowedProrated
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				LaytimeAllowedProrated = ProrationPercentage*LaytimeAllowed;
	end try
	begin catch
		select @ErrorMsg = 'Updating SOFEvent Duration/LaytimeUsed/LaytimeAllowedProrated - ' + error_message();
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
							Parcels p with (nolock)
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
				ParcelProducts pp with (nolock)
					join Warehouse.Dim_Product wproduct with (nolock)
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
					evt.ChartererKey,
					evt.OwnerKey,
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
					evt.LaytimeUsed,
					evt.LaytimeAllowed,
					evt.LaytimeAllowedProrated,
					evt.ParcelQuantity,
					getdate() RowStartDate
				from
					Staging.Fact_SOFEvent evt with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end