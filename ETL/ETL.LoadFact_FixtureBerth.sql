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

	begin try
		-- Get Unique FixtureBerth records
		with UniqueFixtureBerthProducts	(
											RelatedSpiFixtureId,
											RelatedPortId,
											RelatedBerthId,
											RelatedLDPId,
											ParcelAlternateKey,
											ProductAlternateKey,
											ParcelQuantity										
										)
		as
		(
			select
				distinct
					pb.RelatedSpiFixtureId,
					pb.RelatedPortId,
					pb.RelatedBerthId,
					pb.RelatedLDPId,
					p.QBRecId,
					prod.QBRecId,
					p.BLQty
				from
					SOFEvents e
						join ParcelBerths pb
							on e.RelatedParcelBerthId = pb.QBRecId
						join Parcels p
							on p.RelatedSpiFixtureId = pb.RelatedSpiFixtureId
						join ParcelProducts pp 
							on pp.QBRecId = p.RelatedParcelProductId
						join Products prod
							on prod.QBRecId = pp.RelatedProductId
				where
					pb.RelatedSpiFixtureId is not null
					and pb.RelatedPortId is not null
					and pb.RelatedBerthId is not null
		)

		insert
				Staging.Fact_FixtureBerth with (tablock)
											(
												PostFixtureAlternateKey,
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
					ufb.RelatedSpiFixtureId						PostFixtureAlternateKey,
					ufb.RelatedBerthId							BerthAlternateKey,
					ufb.RelatedLDPId							LoadDischargeAlternateKey,
					ufb.ParcelAlternateKey						ParcelAlternateKey,
					--isnull([port].PortKey, -1)					PortKey,
					--isnull(berth.BerthKey, -1)					BerthKey,
					isnull(portberth.PortBerthKey, -1)			PortBerthKey,
					isnull(product.ProductKey, -1)				ProductKey,
					isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
					isnull(vessel.VesselKey, -1)				VesselKey,
					-1											FirstEventDateKey,
					isnull(parcel.ParcelKey, -1)				ParcelKey,
					pp.[Type]									LoadDischarge,
					ufb.ParcelQuantity							ParcelQuantity,
					isnull(rs.RecordStatus, @NewRecord)			RecordStatus
				from
					UniqueFixtureBerthProducts ufb
						join ParcelPorts pp
							on ufb.RelatedLDPId = pp.QBRecId
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = ufb.RelatedSpiFixtureId
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join Warehouse.Dim_PortBerth portberth
							on portberth.PortAlternateKey = ufb.RelatedPortId
								and portberth.BerthAlternateKey = ufb.RelatedBerthId
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
												BerthAlternateKey,
												LoadDischargeAlternateKey,
												ParcelAlternateKey
											from
												Warehouse.Fact_FixtureBerth
									) rs
							on rs.PostFixtureAlternateKey = ufb.RelatedSpiFixtureId
								and rs.BerthAlternateKey = ufb.RelatedBerthId
								and rs.LoadDischargeAlternateKey = ufb.RelatedLDPId
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

/*	-- Update ProductKey
	begin try
		update
				Staging.Fact_SOFEvent
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

	-- Insert new events into Warehouse table
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