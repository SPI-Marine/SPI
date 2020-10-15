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
Brian Boswick	02/05/2020	Added ChartererKey and OwnerKey ETL logic
Brian Boswick	02/11/2020	Added VesselKey ETL logic
Brian Boswick	02/14/2020	Added ProductQuantityKey ETL logic
Brian Boswick	02/20/2020	Added BunkerCharge ETL logic
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	07/30/2020	Added NOR/Hose Off dates for load/discharge ports
Brian Boswick	08/21/2020	Changed ProductQuantityKey logic to aggregate to Fixture level
Brian Boswick	09/28/2020	Added BaseFreightPMT and BunkerAdjustmentPMT ETL logic
Brian Boswick	10/12/2020	Added SupplierName/ReceiverName
Brian Boswick	10/15/2020	Adjusted Product Quantity calculation to include Nominated Quantity when
							BL Quantity is not available
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
														CPDateKey,
														ChartererKey,
														OwnerKey,
														VesselKey,
														ProductFixtureQuantityKey,
														COAKey,
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
														SupplierName,
														ReceiverName,
														LoadPortAlternateKey,
														DischargePortAlternateKey,
														PostFixtureAlternateKey
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
				isnull(cpdate.DateKey, -1)						CPDateKey,
				isnull(wch.ChartererKey, -1)					ChartererKey,
				isnull(wo.OwnerKey, -1)							OwnerKey,
				isnull(v.VesselKey, -1)							VesselKey,
				-1												ProductFixtureQuantityKey,
				isnull(coa.COAKey, -1)							COAKey,
				p.OutTurnQty,
				p.ShipLoadedQty,
				p.ShipDischargeQty,
				p.NominatedQty,
				p.BLQty,
				p.ParcelFreightAmountQBC,
				p.DemurrageVaultEstimateAmount_QBC,
				p.DemurrageAgreedAmount_QBC,
				p.DemurrageClaimAmount_QBC,
				p.DeadfreightQty,
				p.SupplierName,
				p.ReceiverName,
				wdloadport.PortAlternateKey						LoadPortAlternateKey,
				wddischport.PortAlternateKey					DischargePortAlternateKey,
				wdpostfixture.PostFixtureAlternateKey
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
					left join Warehouse.Dim_Calendar cpdate with (nolock)
						on cpdate.FullDate = wdpostfixture.CPDate
					left join Warehouse.Dim_Calendar bld with (nolock)
						on bld.FullDate = convert(date, p.BillLadingDate)
					left join PostFixtures pf with (nolock)
						on p.RelatedSpiFixtureId = pf.QBRecId
					left join Warehouse.Dim_COA coa (nolock)
						on coa.COAAlternateKey = pf.RelatedSPICOAId
					left join FullStyles fs with (nolock)
						on pf.RelatedChartererFullStyle = fs.QBRecId
					left join Warehouse.Dim_Owner wo with (nolock)
						on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
					left join Warehouse.Dim_Charterer wch with (nolock)
						on wch.ChartererAlternateKey = fs.RelatedChartererParentID
					left join Warehouse.Dim_Vessel v with (nolock)
						on v.VesselAlternateKey = pf.RelatedVessel;
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
						ParcelBerths loadberth with (nolock)
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
						ParcelBerths dischberth with (nolock)
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
						ParcelBerths loadberth with (nolock)
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
						ParcelBerths dischberth with (nolock)
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
						sum	(
								case
									when isnull(p.BLQty, 0.0) = 0.0
										then p.NominatedQty
									else p.BLQty
								end
							) TotalBerthBLQty
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
						sum	(
								case
									when isnull(p.BLQty, 0.0) = 0.0
										then p.NominatedQty
									else p.BLQty
								end
							) TotalBerthBLQty
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
			),
			AggregateFixtureBLQty	(
										PostFixtureKey,
										TotalFixtureBLQty
									)
			as
			(
				select
						wpf.PostFixtureKey,
						sum	(
								case
									when isnull(p.BLQty, 0.0) = 0.0
										then p.NominatedQty
									else p.BLQty
								end
							) TotalFixtureBLQty
					from
						Parcels p with (nolock)
							join Warehouse.Dim_PostFixture wpf with (nolock)
								on wpf.PostFixtureAlternateKey = p.RelatedSpiFixtureId
					group by
						wpf.PostFixtureKey
			)
		
		update
				Staging.Fact_Parcel with (tablock)
			set
				TotalLoadBerthBLQty = lt.TotalBerthBLQty,
				TotalDischargeBerthBLQty = dt.TotalBerthBLQty,
				ProductFixtureQuantityKey = isnull(dpq.ProductQuantityKey, -1)
			from
				Staging.Fact_Parcel sfp
					left join AggregateLoadTotalBerthBLQty lt
						on sfp.PostFixtureKey = lt.PostFixtureKey
							and sfp.LoadBerthKey = lt.BerthKey
					left join AggregateDischargeBerthBLQty dt
						on sfp.PostFixtureKey = dt.PostFixtureKey
							and sfp.DischargeBerthKey = dt.BerthKey
					left join AggregateFixtureBLQty fpq
						on fpq.PostFixtureKey = sfp.PostFixtureKey
					left join Warehouse.Dim_ProductQuantity dpq
						on convert(decimal(18, 2), fpq.TotalFixtureBLQty) between dpq.MinimumQuantity and dpq.MaximumQuantity;
	end try
	begin catch
		select @ErrorMsg = 'Updating Berth/Fixture BL Quantity metrics - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Laytime/BLQty Ratio metrics
	begin try
		update
				Staging.Fact_Parcel with (tablock)
			set
				LoadLaytimeAllowed = (BLQty/TotalLoadBerthBLQty)*LoadLaytimeAllowed,
				LoadLaytimeUsed = (BLQty/TotalLoadBerthBLQty)*LoadLaytimeUsed,
				DischargeLaytimeAllowed = (BLQty/TotalDischargeBerthBLQty)*DischargeLaytimeAllowed,
				DischargeLaytimeUsed = (BLQty/TotalDischargeBerthBLQty)*DischargeLaytimeUsed
			where
				isnull(TotalLoadBerthBLQty, 0) > 0
				and isnull(TotalDischargeBerthBLQty, 0) > 0;
	end try
	begin catch
		select @ErrorMsg = 'Updating Laytime/BLQty Ratio metrics - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Get NOR start dates for load/discharge ports
	begin try
		update
				Staging.Fact_Parcel with (tablock)
			set
				LoadNORStartDate = firstloadnorevent.FirstNOREventDate,
				LoadLastHoseOffDate = lastloadhoseoffevent.LastHoseOffEventDate,
				DischargeNORStartDate = firstdischargenorevent.FirstNOREventDate,
				DischargeLastHoseOffDate = lastdischargehoseoffevent.LastHoseOffEventDate
			from
				Staging.Fact_Parcel sfp
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											min(e.StartDate)	FirstNOREventDate
										from
											SOFEvents e with (nolock)
												join PortEventTimes pet with (nolock)
													on pet.QBRecId = e.RelatedPortTimeEventId
												join ParcelBerths pb with (nolock)
													on pb.QBRecId = e.RelatedParcelBerthId
												join ParcelPorts pp with (nolock)
													on pp.QBRecId = pb.RelatedLDPId
												join PostFixtures pf with (nolock)
													on pf.QBRecId = pb.RelatedSpiFixtureId
												where
													pet.QBRecId = 219	-- NOR
													and pp.[Type] = 'Load'
												group by
													pf.QBRecId, pp.RelatedPortId
								) firstloadnorevent
						on firstloadnorevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and firstloadnorevent.RelatedPortID = sfp.LoadPortAlternateKey
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											max(e.StartDate)	LastHoseOffEventDate
										from
											SOFEvents e with (nolock)
												join PortEventTimes pet with (nolock)
													on pet.QBRecId = e.RelatedPortTimeEventId
												join ParcelBerths pb with (nolock)
													on pb.QBRecId = e.RelatedParcelBerthId
												join ParcelPorts pp with (nolock)
													on pp.QBRecId = pb.RelatedLDPId
												join PostFixtures pf with (nolock)
													on pf.QBRecId = pb.RelatedSpiFixtureId
												where
													pet.QBRecId = 260	-- Hose Off Event
													and pp.[Type] = 'Load'
												group by
													pf.QBRecId, pp.RelatedPortId
								) lastloadhoseoffevent
						on lastloadhoseoffevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and lastloadhoseoffevent.RelatedPortID = sfp.LoadPortAlternateKey
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											min(e.StartDate)	FirstNOREventDate
										from
											SOFEvents e with (nolock)
												join PortEventTimes pet with (nolock)
													on pet.QBRecId = e.RelatedPortTimeEventId
												join ParcelBerths pb with (nolock)
													on pb.QBRecId = e.RelatedParcelBerthId
												join ParcelPorts pp with (nolock)
													on pp.QBRecId = pb.RelatedLDPId
												join PostFixtures pf with (nolock)
													on pf.QBRecId = pb.RelatedSpiFixtureId
												where
													pet.QBRecId = 219	-- NOR
													and pp.[Type] = 'Discharge'
												group by
													pf.QBRecId, pp.RelatedPortId
								) firstdischargenorevent
						on firstdischargenorevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and firstdischargenorevent.RelatedPortID = sfp.DischargePortAlternateKey
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											max(e.StartDate)	LastHoseOffEventDate
										from
											SOFEvents e with (nolock)
												join PortEventTimes pet with (nolock)
													on pet.QBRecId = e.RelatedPortTimeEventId
												join ParcelBerths pb with (nolock)
													on pb.QBRecId = e.RelatedParcelBerthId
												join ParcelPorts pp with (nolock)
													on pp.QBRecId = pb.RelatedLDPId
												join PostFixtures pf with (nolock)
													on pf.QBRecId = pb.RelatedSpiFixtureId
												where
													pet.QBRecId = 260	-- Hose Off Event
													and pp.[Type] = 'Discharge'
												group by
													pf.QBRecId, pp.RelatedPortId
								) lastdischargehoseoffevent
						on lastdischargehoseoffevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and lastdischargehoseoffevent.RelatedPortID = sfp.DischargePortAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating NOR start dates for load/discharge ports - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Calculate BunkerCharge for the Post Fixture and pro-rate it by the number of Parcels
	begin try
		with PostFixtureParcelCount	(
										PostFixtureAlternateKey,
										ParcelCount
									)
		as
		(
			select
					p.RelatedSpiFixtureId	PostFixtureAlternateKey,
					count(p.QBRecId)		ParcelCount
				from
					Parcels p with (nolock)
				group by
					p.RelatedSpiFixtureId
		),
		PostFixtureBunkerCharges	(
										PostFixtureAlternateKey,
										BunkerCharge
									)
		as
		(
			select
					ac.RelatedSPIFixtureId		PostFixtureAlternateKey,
					sum(ac.Amount)				BunkerCharge
				from
					AdditionalCharges ac with (nolock)
						join AdditionalChargeType chgtype
							on chgtype.RecordID = ac.RelatedAdditionalChargeType
				where
					chgtype.ChargeType = 'Bunker Adjustment'
				group by
					ac.RelatedSPIFixtureId
		)

		update
				Staging.Fact_Parcel with (tablock)
			set
				BunkerCharge = bc.BunkerCharge/ pc.ParcelCount
			from
				Staging.Fact_Parcel fp
					join PostFixtureBunkerCharges bc
						on bc.PostFixtureAlternateKey = fp.PostFixtureAlternateKey
					join PostFixtureParcelCount pc
						on pc.PostFixtureAlternateKey = fp.PostFixtureAlternateKey
			where
				pc.ParcelCount > 0
	end try
	begin catch
		select @ErrorMsg = 'Calculating BunkerCharge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
	
	-- Calculate BaseFreightPMT/BunkerAdjustmentPMT for the Parcel
	begin try
		update
				Staging.Fact_Parcel with (tablock)
			set
				BaseFreightPMT = ParcelFreightAmountQBC/BLQty,
				BunkerAdjustmentPMT = BunkerCharge/BLQty
			where
				BLQty > 0;
	end try
	begin catch
		select @ErrorMsg = 'Calculating BaseFreightPMT/BunkerAdjustmentPMT - ' + error_message();
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
															CPDateKey,
															ChartererKey,
															OwnerKey,
															VesselKey,
															ProductFixtureQuantityKey,
															COAKey,
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
															BunkerCharge,
															BaseFreightPMT,
															BunkerAdjustmentPMT,
															LoadNORStartDate,
															LoadLastHoseOffDate,
															DischargeNORStartDate,
															DischargeLastHoseOffDate,
															SupplierName,
															ReceiverName,
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
					sfp.CPDateKey,
					sfp.ChartererKey,
					sfp.OwnerKey,
					sfp.VesselKey,
					sfp.ProductFixtureQuantityKey,
					sfp.COAKey,
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
					sfp.BunkerCharge,
					sfp.BaseFreightPMT,
					sfp.BunkerAdjustmentPMT,
					sfp.LoadNORStartDate,
					sfp.LoadLastHoseOffDate,
					sfp.DischargeNORStartDate,
					sfp.DischargeLastHoseOffDate,
					sfp.SupplierName,
					sfp.ReceiverName,
					getdate()
				from
					Staging.Fact_Parcel sfp with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end