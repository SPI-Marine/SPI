set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_Parcel;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/16/2019
Description:	Creates the LoadFact_Parcel stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_Parcel
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_Parcel', 'U') is not null
		truncate table Staging.Fact_Parcel;

	begin try
		insert
				Staging.Fact_Parcel with (tablock)	(
														ParcelAlternateKey,
														PostFixtureKey,
														LoadPortKey,
														DischargePortKey,
														LoadBerthKey,
														DischargeBerthKey,
														LoadPortBerthKey,
														DischargePortBerthKey,
														ProductKey,
														BillLadingDateKey,
														DimParcelKey,
														OutTurnQty,
														ShipLoadedQty,
														ShipDischargeQty,
														NominatedQty,
														BLQty,
														ParcelFreightAmountQBC,
														DemurrageVaultEstimateAmount_QBC,
														DemurrageAgreedAmount_QBC,
														DemurrageClaimAmount_QBC,
														DeadfreightQty
													)
		select
				p.QbRecId										ParcelAlternateKey,
				isnull(wdpostfixture.PostFixtureKey, -1)		PostFixtureKey,
				isnull(wdloadport.PortKey, -1)					LoadPortKey,
				isnull(wddischport.PortKey, -1)					DischargePortKey,
				isnull(wdloadberth.BerthKey, -1)				LoadBerthKey,
				isnull(wddischberth.BerthKey, -1)				DischargeBerthKey,
				isnull(wdloadportberth.PortBerthKey, -1)		LoadPortBerthKey,
				isnull(wddischportberth.PortBerthKey, -1)		DischargePortBerthKey,
				isnull(wdproduct.ProductKey , -1)				ProductKey,
				isnull(bld.DateKey, -1)							BillLadingDateKey,
				wdparcel.ParcelKey								DimParcelKey,
				p.OutTurnQty,
				p.ShipLoadedQty,
				p.ShipDischargeQty,
				p.NominatedQty,
				p.BLQty,
				p.ParcelFreightAmountQBC,
				p.DemurrageVaultEstimateAmount_QBC,
				p.DemurrageAgreedAmount_QBC,
				p.DemurrageClaimAmount_QBC,
				p.DeadfreightQty
			from
				Parcels p with (nolock)
					join Warehouse.Dim_Parcel wdparcel with (nolock)
						on wdparcel.ParcelAlternateKey = p.QbRecId
					left join ParcelPorts loadparcelport with (nolock)
						on loadparcelport.QBRecId = p.RelatedLoadPortID
					left join Warehouse.Dim_Port wdloadport with (nolock)
						on wdloadport.PortAlternateKey = loadparcelport.RelatedPortId
					left join ParcelPorts dischparcelport with (nolock)
						on dischparcelport.QBRecId = p.RelatedDischPortId
					left join Warehouse.Dim_Port wddischport with (nolock)
						on wddischport.PortAlternateKey = dischparcelport.RelatedPortId
					left join ParcelBerths loadparcelberth with (nolock)
						on loadparcelberth.QBRecId = p.RelatedLoadBerth
					left join Warehouse.Dim_Berth wdloadberth with (nolock)
						on wdloadberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
					left join ParcelBerths dischparcelberth with (nolock)
						on dischparcelberth.QBRecId = p.RelatedDischBerth
					left join Warehouse.Dim_Berth wddischberth with (nolock)
						on wddischberth.BerthAlternateKey = dischparcelberth.RelatedBerthId
					left join Warehouse.Dim_PortBerth wdloadportberth with (nolock)
						on wdloadportberth.PortAlternateKey = loadparcelport.RelatedPortId
							and wdloadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
					left join Warehouse.Dim_PortBerth wddischportberth with (nolock)
						on wddischportberth.PortAlternateKey = dischparcelport.RelatedPortId
							and wddischportberth.BerthAlternateKey = dischparcelberth.RelatedBerthId
					left join ParcelProducts parprod with (nolock)
						on parprod.QBRecId = p.RelatedParcelProductId
					left join Warehouse.Dim_Product wdproduct with (nolock)
						on wdproduct.ProductAlternateKey = parprod.RelatedProductId
					left join Warehouse.Dim_PostFixture wdpostfixture with (nolock)
						on p.RelatedSpiFixtureId = wdpostfixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Calendar bld with (nolock)
						on bld.FullDate = convert(date, p.BillLadingDate);
	end try
	begin catch
		select @ErrorMsg = 'Staging Parcel records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Calculate LaytimeUsed at the Berth
	begin try
		with
			AggregateLoadBerthLaytimeUsed	(
												PostFixtureKey,
												BerthKey,
												LaytimeUsed
											)
			as
			(
				select
						wpf.PostFixtureKey,
						wloadberth.BerthKey,
						sum(ee.LtUsedProrationAmtHrs_QBC) LaytimeUsed
					from
						Parcels p with (nolock)
							join ParcelBerths loadberth with (nolock)
								on p.RelatedLoadBerth = loadberth.QBRecId
							join SOFEvents ee with (nolock)
								on ee.RelatedParcelBerthId = loadberth.QBRecId
							join Warehouse.Dim_PostFixture wpf with (nolock)
								on wpf.PostFixtureAlternateKey = loadberth.RelatedSpiFixtureId
							join Warehouse.Dim_Berth wloadberth with (nolock)
								on wloadberth.BerthAlternateKey = loadberth.RelatedBerthId
					group by
						wpf.PostFixtureKey,
						wloadberth.BerthKey
			),
			AggregateDischargeBerthLaytimeUsed	(
												PostFixtureKey,
												BerthKey,
												LaytimeUsed
											)
			as
			(
				select
						wpf.PostFixtureKey,
						wdischberth.BerthKey,
						sum(ee.LtUsedProrationAmtHrs_QBC) LaytimeUsed
					from
						Parcels p with (nolock)
							join ParcelBerths dischberth with (nolock)
								on p.RelatedDischBerth = dischberth.QBRecId
							join SOFEvents ee with (nolock)
								on ee.RelatedParcelBerthId = dischberth.QBRecId
							join Warehouse.Dim_PostFixture wpf with (nolock)
								on wpf.PostFixtureAlternateKey = dischberth.RelatedSpiFixtureId
							join Warehouse.Dim_Berth wdischberth with (nolock)
								on wdischberth.BerthAlternateKey = dischberth.RelatedBerthId
					group by
						wpf.PostFixtureKey,
						wdischberth.BerthKey
			),
			AggregateLoadBerthLaytimeAllowed	(
													PostFixtureKey,
													BerthKey,
													LaytimeAllowed
												)
			as
			(
				select
						wpf.PostFixtureKey,
						wloadberth.BerthKey,
						sum(loadberth.LaytimeAllowedBerthHrs_QBC) LaytimeAllowed
					from
						Parcels p with (nolock)
							join ParcelBerths loadberth with (nolock)
								on p.RelatedLoadBerth = loadberth.QBRecId
							join Warehouse.Dim_PostFixture wpf with (nolock)
								on wpf.PostFixtureAlternateKey = loadberth.RelatedSpiFixtureId
							join Warehouse.Dim_Berth wloadberth with (nolock)
								on wloadberth.BerthAlternateKey = loadberth.RelatedBerthId
					group by
						wpf.PostFixtureKey,
						wloadberth.BerthKey
			),
			AggregateDischargeBerthLaytimeAllowed	(
														PostFixtureKey,
														BerthKey,
														LaytimeAllowed
													)
			as
			(
				select
						wpf.PostFixtureKey,
						wdischberth.BerthKey,
						sum(dischberth.LaytimeAllowedBerthHrs_QBC) LaytimeAllowed
					from
						Parcels p with (nolock)
							join ParcelBerths dischberth with (nolock)
								on p.RelatedDischBerth = dischberth.QBRecId
							join Warehouse.Dim_PostFixture wpf with (nolock)
								on wpf.PostFixtureAlternateKey = dischberth.RelatedSpiFixtureId
							join Warehouse.Dim_Berth wdischberth with (nolock)
								on wdischberth.BerthAlternateKey = dischberth.RelatedBerthId
					group by
						wpf.PostFixtureKey,
						wdischberth.BerthKey
			)

		update
				Staging.Fact_Parcel with (tablock)
			set
				LoadLaytimeAllowed = lbla.LaytimeAllowed,
				LoadLaytimeUsed = lblu.LaytimeUsed,
				DischargeLaytimeAllowed = dbla.LaytimeAllowed,
				DischargeLaytimeUsed = dblu.LaytimeUsed
			from
				Staging.Fact_Parcel sfp
					left join AggregateLoadBerthLaytimeUsed lblu
						on sfp.PostFixtureKey = lblu.PostFixtureKey
							and sfp.LoadBerthKey = lblu.BerthKey
					left join AggregateLoadBerthLaytimeAllowed lbla
						on sfp.PostFixtureKey = lbla.PostFixtureKey
							and sfp.LoadBerthKey = lbla.BerthKey
					left join AggregateDischargeBerthLaytimeUsed dblu
						on sfp.PostFixtureKey = dblu.PostFixtureKey
							and sfp.DischargeBerthKey = dblu.BerthKey
					left join AggregateDischargeBerthLaytimeAllowed dbla
						on sfp.PostFixtureKey = dbla.PostFixtureKey
							and sfp.DischargeBerthKey = dbla.BerthKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating Berth Laytime metrics - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Calculate TotalBerthBLQty
	begin try
		with
			AggregateLoadTotalBerthBLQty	(
												PostFixtureKey,
												BerthKey,
												TotalBerthBLQty
											)
			as
			(
				select
						wpf.PostFixtureKey,
						wloadberth.BerthKey,
						sum(p.BLQty) TotalBerthBLQty
					from
						Parcels p with (nolock)
							join ParcelBerths loadberth with (nolock)
								on p.RelatedLoadBerth = loadberth.QBRecId
							join Warehouse.Dim_PostFixture wpf with (nolock)
								on wpf.PostFixtureAlternateKey = loadberth.RelatedSpiFixtureId
							join Warehouse.Dim_Berth wloadberth with (nolock)
								on wloadberth.BerthAlternateKey = loadberth.RelatedBerthId
					group by
						wpf.PostFixtureKey,
						wloadberth.BerthKey
			),
			AggregateDischargeBerthBLQty	(
												PostFixtureKey,
												BerthKey,
												TotalBerthBLQty
											)
			as
			(
				select
						wpf.PostFixtureKey,
						wdischberth.BerthKey,
						sum(p.BLQty) TotalBerthBLQty
					from
						Parcels p with (nolock)
							join ParcelBerths dischberth with (nolock)
								on p.RelatedDischBerth = dischberth.QBRecId
							join Warehouse.Dim_PostFixture wpf with (nolock)
								on wpf.PostFixtureAlternateKey = dischberth.RelatedSpiFixtureId
							join Warehouse.Dim_Berth wdischberth with (nolock)
								on wdischberth.BerthAlternateKey = dischberth.RelatedBerthId
					group by
						wpf.PostFixtureKey,
						wdischberth.BerthKey
			)
		
		update
				Staging.Fact_Parcel with (tablock)
			set
				TotalLoadBerthBLQty = lt.TotalBerthBLQty,
				TotalDischargeBerthBLQty = dt.TotalBerthBLQty
			from
				Staging.Fact_Parcel sfp
					left join AggregateLoadTotalBerthBLQty lt
						on sfp.PostFixtureKey = lt.PostFixtureKey
							and sfp.LoadBerthKey = lt.BerthKey
					left join AggregateDischargeBerthBLQty dt
						on sfp.PostFixtureKey = dt.PostFixtureKey
							and sfp.DischargeBerthKey = dt.BerthKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating Berth BL Quantity metrics - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	begin try
		update
				Staging.Fact_Parcel with (tablock)
			set
				LoadLaytimeAllowed = LoadLaytimeAllowed/TotalLoadBerthBLQty,
				LoadLaytimeUsed = LoadLaytimeUsed/TotalLoadBerthBLQty,
				DischargeLaytimeAllowed = DischargeLaytimeAllowed/TotalDischargeBerthBLQty,
				DischargeLaytimeUsed = DischargeLaytimeUsed/TotalDischargeBerthBLQty
			where
				isnull(TotalLoadBerthBLQty, 0) > 0
				and isnull(TotalDischargeBerthBLQty, 0) > 0;
	end try
	begin catch
		select @ErrorMsg = 'Updating Laytime/BLQty Ratios metrics - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_Parcel', 'U') is not null
		truncate table Warehouse.Fact_Parcel;

	-- Insert records into Warehouse table
	begin try
		insert
				Warehouse.Fact_Parcel with (tablock)	(
															ParcelAlternateKey,
															PostFixtureKey,
															LoadPortKey,
															DischargePortKey,
															LoadBerthKey,
															DischargeBerthKey,
															LoadPortBerthKey,
															DischargePortBerthKey,
															ProductKey,
															BillLadingDateKey,
															DimParcelKey,
															OutTurnQty,
															ShipLoadedQty,
															ShipDischargeQty,
															NominatedQty,
															BLQty,
															ParcelFreightAmountQBC,
															DemurrageVaultEstimateAmount_QBC,
															DemurrageAgreedAmount_QBC,
															DemurrageClaimAmount_QBC,
															DeadfreightQty,
															LoadLaytimeAllowed,
															LoadLaytimeUsed,
															DischargeLaytimeAllowed,
															DischargeLaytimeUsed,
															RowCreatedDate
														)
			select
					sfp.ParcelAlternateKey,
					sfp.PostFixtureKey,
					sfp.LoadPortKey,
					sfp.DischargePortKey,
					sfp.LoadBerthKey,
					sfp.DischargeBerthKey,
					sfp.LoadPortBerthKey,
					sfp.DischargePortBerthKey,
					sfp.ProductKey,
					sfp.BillLadingDateKey,
					sfp.DimParcelKey,
					sfp.OutTurnQty,
					sfp.ShipLoadedQty,
					sfp.ShipDischargeQty,
					sfp.NominatedQty,
					sfp.BLQty,
					sfp.ParcelFreightAmountQBC,
					sfp.DemurrageVaultEstimateAmount_QBC,
					sfp.DemurrageAgreedAmount_QBC,
					sfp.DemurrageClaimAmount_QBC,
					sfp.DeadfreightQty,
					sfp.LoadLaytimeAllowed,
					sfp.LoadLaytimeUsed,
					sfp.DischargeLaytimeAllowed,
					sfp.DischargeLaytimeUsed,
					getdate()
				from
					Staging.Fact_Parcel sfp with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end