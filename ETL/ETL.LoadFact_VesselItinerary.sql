set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_VesselItinerary;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/13/2019
Description:	Creates the LoadFact_VesselItinerary stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_VesselItinerary
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_VesselItinerary', 'U') is not null
		truncate table Staging.Fact_VesselItinerary;

	begin try
		insert
				Staging.Fact_VesselItinerary with (tablock)
		select
			distinct
				vi.RecordID									VesselItineraryAlternateKey,
				isnull(fixture.PostFixtureKey, -1)			PostFixtureKey,
				-1											PortKey,
				isnull(sd.DateKey, 18991230)				ETAStartDateKey,
				coalesce(ed.DateKey, sd.DateKey, 18991230)	ETAEndDateKey,
				isnull(dm.DateKey, 47001231)				DateModifiedKey,
				vi.ItineraryPortType						ItineraryPortType,
				vi.Comments									Remarks,
				vi.RelatedParcelPortID						RelatedParcelPortID,
				vi.RelatedPortID							RelatedPortID
			from
				VesselItinerary vi with (nolock)
					left join Warehouse.Dim_PostFixture fixture with (nolock)
						on vi.RelatedSPIFixtureID = fixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Calendar sd with (nolock)
						on sd.FullDate = try_convert(date, vi.ETAStart)
					left join Warehouse.Dim_Calendar ed with (nolock)
						on ed.FullDate = try_convert(date, vi.ETAEnd)
					left join Warehouse.Dim_Calendar dm
						on dm.FullDate = try_convert(date, vi.DateModified);

		-- Insert NOR Tendered records
		insert
				Staging.Fact_VesselItinerary with (tablock)
		select
			distinct
				(e.QBRecId * -1)							VesselItineraryAlternateKey,
				isnull(fixture.PostFixtureKey, -1)			PostFixtureKey,
				-1											PortKey,
				isnull(sd.DateKey, 18991230)				ETAStartDateKey,
				isnull(sd.DateKey, 18991230)				ETAEndDateKey,
				isnull(dm.DateKey, 47001231)				DateModifiedKey,
				'NOR Tendered'								ItineraryPortType,
				null										Remarks,
				null										RelatedParcelPortID,
				eventport.RelatedPortId						RelatedPortID
			from
				SOFEvents e with (nolock)
					left join PortEventTimes eventtype with (nolock)
						on eventtype.QBRecId = e.RelatedPortTimeEventId
					left join ParcelBerths pb with (nolock)
						on e.RelatedParcelBerthId = pb.QBRecId
					join ParcelPorts eventport with (nolock)
						on eventport.QBRecId = pb.RelatedLDPId
					left join Warehouse.Dim_PostFixture fixture with (nolock)
						on pb.RelatedSpiFixtureId = fixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Calendar sd with (nolock)
						on sd.FullDate = try_convert(date, e.StartDate)
					left join Warehouse.Dim_Calendar dm
						on dm.FullDate = try_convert(date, e.DateModified)
			where
				eventtype.EventNameReports like 'NOR Tendered';
	end try
	begin catch
		select @ErrorMsg = 'Staging VesselItinerary records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Update PortKey
	begin try
		update
				Staging.Fact_VesselItinerary
			set
				PortKey =	case
								when vi.ItineraryPortType like 'Fixture%'
									then isnull(fpt.PortKey, -1)
								--when vi.ItineraryPortType like 'NOR%'
								--	then 1
								when vi.ItineraryPortType like 'Itinerary%' or vi.ItineraryPortType like 'NOR%'
									then isnull(ipt.PortKey, -1)
								else -1
							end
			from
				Staging.Fact_VesselItinerary vi
					left join Warehouse.Dim_Port ipt with (nolock)
						on ipt.PortAlternateKey = vi.RelatedPortID
					left join ParcelPorts pp with (nolock)
						on pp.QBRecId = vi.RelatedParcelPortID
					left join Warehouse.Dim_Port fpt with (nolock)
						on fpt.PortAlternateKey = pp.RelatedPortId
					--left join Warehouse.Dim_Port npt
					--	on npt.PortAlternateKey = vi.RelatedPortID
			where
				vi.VesselItineraryAlternateKey = VesselItineraryAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging VesselItinerary records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_VesselItinerary', 'U') is not null
		truncate table Warehouse.Fact_VesselItinerary;
	
	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_VesselItinerary with (tablock)
			select
					fvi.VesselItineraryAlternateKey,
					fvi.PostFixtureKey,
					fvi.PortKey,
					fvi.ETAStartDateKey,
					fvi.ETAEndDateKey,
					fvi.DateModifiedKey,
					fvi.ItineraryPortType,
					fvi.Comments,
					getdate() RowStartDate
				from
					Staging.Fact_VesselItinerary fvi;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end