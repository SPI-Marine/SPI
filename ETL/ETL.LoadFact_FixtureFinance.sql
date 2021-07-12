set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_FixtureFinance;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	01/08/2019
Description:	Creates the LoadFact_FixtureFinance stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey ETL logic
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	06/18/2021	Added ProductFixtureQuantityKey
Brian Boswick	07/12/2021	Removed COAKey
==========================================================================================================	
*/

create procedure ETL.LoadFact_FixtureFinance
as
begin

	declare	@NewRecord				smallint = 1,
			@ExistingRecord			smallint = 2,
			@ErrorMsg				varchar(1000),
			@FreightChargeType		smallint = 1,
			@DemurrageChargeType	smallint = 2,
			@ParcelChargeType		smallint = 3,
			@AdditionalChargeType	smallint = 4;

	-- Clear Staging table
	if object_id(N'Staging.Fact_FixtureFinance', 'U') is not null
		truncate table Staging.Fact_FixtureFinance;

	-- Get Freight Charges
	begin try
		insert
				Staging.Fact_FixtureFinance with (tablock)
			select
				distinct
					parcel.RelatedSPIFixtureId									PostFixtureAlternateKey,
					-1															RebillAlternateKey,
					-1															ChargeAlternateKey,
					isnull(parcel.RelatedParcelProductId, -1)					ParcelProductAlternateKey,
					isnull(parprod.RelatedProductId, -1)						ProductAlternateKey,
					parcel.QbRecId												ParcelAlternateKey,
					@FreightChargeType											ChargeTypeAlternateKey,
					isnull(loadportberth.PortBerthKey, -1)						LoadPortBerthKey,
					isnull(dischargeportberth.PortBerthKey, -1)					DischargePortBerthKey,
					isnull(loadport.PortKey, -1)								LoadPortKey,
					isnull(dischargeport.PortKey, -1)							DischargePortKey,
					isnull(wproduct.ProductKey, -1)								ProductKey,
					isnull(wparcel.ParcelKey, -1)								ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)						PostFixtureKey,
					isnull(vessel.VesselKey, -1)								VesselKey,
					isnull(cpdate.DateKey, -1)									CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)							FirstLoadEventDateKey,
					isnull(wch.ChartererKey, -1)								ChartererKey,
					isnull(wo.OwnerKey, -1)										OwnerKey,
					isnull(pq.ProductQuantityKey, -1)							ProductFixtureQuantityKey,
					'Freight'													ChargeType,
					null														ChargeDescription,
					null														ParcelNumber,
					parcel.ParcelFreightAmountQBC								Charge,
					case
						when isnull(parcel.BLQty, 0) > 0
							then parcel.ParcelFreightAmountQBC/parcel.BLQty
						else null
					end															ChargePerMetricTon,
					try_convert	(
									decimal(20, 8),
									epostfixture.AddressCommissionPercent
								) / 100											AddressCommissionRate,
					case
						when isnull(epostfixture.Add_Comm_applies_to_Frt_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then parcel.ParcelFreightAmountQBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)
						else null
					end															AddressCommissionAmount,
					case
						when isnull(epostfixture.Add_Comm_applies_to_Frt_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then parcel.ParcelFreightAmountQBC - isnull	(
																			parcel.ParcelFreightAmountQBC - (parcel.ParcelFreightAmountQBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)),
																			0.0
																		)
						else null
					end															AddressCommissionApplied
				from
					Parcels parcel with (nolock)
						left join ParcelProducts parprod with (nolock)
							on parprod.QBRecId = parcel.RelatedParcelProductId
						left join Products product with (nolock)
							on product.QBRecId = parprod.RelatedProductId
						left join Warehouse.Dim_Product wproduct with (nolock)
							on wproduct.ProductAlternateKey = product.QBRecId	
						left join Warehouse.Dim_Parcel wparcel with (nolock)
							on wparcel.ParcelAlternateKey = parcel.QbRecId
						left join ParcelPorts loadparcelport with (nolock)
							on loadparcelport.QBRecId = parcel.RelatedLoadPortID
						left join ParcelBerths loadparcelberth with (nolock)
							on loadparcelberth.QBRecId = parcel.RelatedLoadBerth
						left join ParcelPorts dischargeparcelport with (nolock)
							on dischargeparcelport.QBRecId = parcel.RelatedDischPortId
						left join ParcelBerths dischargeparcelberth with (nolock)
							on dischargeparcelberth.QBRecId = parcel.RelatedDischBerth
						left join Warehouse.Dim_PortBerth loadportberth with (nolock)
							on loadportberth.PortAlternateKey = loadparcelport.RelatedPortId
								and loadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
						left join Warehouse.Dim_PortBerth dischargeportberth with (nolock)
							on dischargeportberth.PortAlternateKey = dischargeparcelport.RelatedPortId
								and dischargeportberth.BerthAlternateKey = dischargeparcelberth.RelatedBerthId
						left join Warehouse.Dim_Port loadport with (nolock)
							on loadport.PortAlternateKey = loadparcelport.RelatedPortId
						left join Warehouse.Dim_Port dischargeport with (nolock)
							on dischargeport.PortAlternateKey = dischargeparcelberth.RelatedPortId
						left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture with (nolock)
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join FullStyles fs with (nolock)
							on epostfixture.RelatedChartererFullStyle = fs.QBRecId
						left join Warehouse.Dim_Owner wo with (nolock)
							on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
						left join Warehouse.Dim_Charterer wch with (nolock)
							on wch.ChartererAlternateKey = fs.RelatedChartererParentID
						left join Warehouse.Dim_Calendar cpdate with (nolock)
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e with (nolock)
													join ParcelBerths pb with (nolock)
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf with (nolock)
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp with (nolock)
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate with (nolock)
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel with (nolock)
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join Warehouse.Dim_ProductQuantity pq with (nolock)
							on parcel.BLQty >= pq.MinimumQuantity
								and parcel.BLQty < pq.MaximumQuantity
				where
					parcel.RelatedSPIFixtureId is not null
					and parcel.ParcelFreightAmountQBC is not null;
	end try
	begin catch
		select @ErrorMsg = 'Staging Parcel Freight records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Get Demurrage Charges
	begin try
		insert
				Staging.Fact_FixtureFinance with (tablock)
			select
				distinct
					parcel.RelatedSPIFixtureId									PostFixtureAlternateKey,
					-2															RebillAlternateKey,
					-2															ChargeAlternateKey,
					isnull(parcel.RelatedParcelProductId, -1)					ParcelProductAlternateKey,
					isnull(parprod.RelatedProductId, -1)						ProductAlternateKey,
					parcel.QbRecId												ParcelAlternateKey,
					@DemurrageChargeType										ChargeTypeAlternateKey,
					isnull(loadportberth.PortBerthKey, -1)						LoadPortBerthKey,
					isnull(dischargeportberth.PortBerthKey, -1)					DischargePortBerthKey,
					isnull(loadport.PortKey, -1)								LoadPortKey,
					isnull(dischargeport.PortKey, -1)							DischargePortKey,
					isnull(wproduct.ProductKey, -1)								ProductKey,
					isnull(wparcel.ParcelKey, -1)								ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)						PostFixtureKey,
					isnull(vessel.VesselKey, -1)								VesselKey,
					isnull(cpdate.DateKey, -1)									CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)							FirstLoadEventDateKey,
					isnull(wch.ChartererKey, -1)								ChartererKey,
					isnull(wo.OwnerKey, -1)										OwnerKey,
					isnull(pq.ProductQuantityKey, -1)							ProductFixtureQuantityKey,
					case
						when parcel.DemurrageAgreedAmount_QBC = 0.0
								and epostfixture.ZeroDemurrage = 1
							then 'Agreed Demurrage'
						when parcel.DemurrageAgreedAmount_QBC <> 0.0
							then 'Agreed Demurrage'
						when parcel.DemurrageClaimAmount_QBC <> 0.0
							then 'Claim Demurrage'
						else 'Vault Demurrage'
					end															ChargeType,
					null														ChargeDescription,
					null														ParcelNumber,
					case
						when parcel.DemurrageAgreedAmount_QBC = 0.0
								and epostfixture.ZeroDemurrage = 1
							then parcel.DemurrageAgreedAmount_QBC
						when parcel.DemurrageAgreedAmount_QBC <> 0.0
							then parcel.DemurrageAgreedAmount_QBC
						when parcel.DemurrageClaimAmount_QBC <> 0.0
							then parcel.DemurrageClaimAmount_QBC
						else parcel.DemurrageVaultEstimateAmount_QBC
					end															Charge,
					case
						when isnull(parcel.BLQty, 0) > 0
							then
									case
										when parcel.DemurrageAgreedAmount_QBC <> 0.0
											then parcel.DemurrageAgreedAmount_QBC/parcel.BLQty
										when parcel.DemurrageClaimAmount_QBC <> 0.0
											then parcel.DemurrageClaimAmount_QBC/parcel.BLQty
										when parcel.DemurrageVaultEstimateAmount_QBC <> 0.0
											then parcel.DemurrageVaultEstimateAmount_QBC/parcel.BLQty
										else null
									end
						else null
					end															ChargePerMetricTon,
					try_convert	(
									decimal(20, 8),
									epostfixture.AddressCommissionPercent
								) / 100											AddressCommissionRate,
					case
						when isnull(epostfixture.Add_Comm_applies_to_Demurrage_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then	case
										when parcel.DemurrageAgreedAmount_QBC <> 0.0
											then parcel.DemurrageAgreedAmount_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)
										when parcel.DemurrageClaimAmount_QBC <> 0.0
											then parcel.DemurrageClaimAmount_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)
										when parcel.DemurrageVaultEstimateAmount_QBC <> 0.0
											then parcel.DemurrageVaultEstimateAmount_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)
										else null
									end
						else null
					end															AddressCommissionAmount,
					case
						when isnull(epostfixture.Add_Comm_applies_to_Demurrage_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then	case
										when parcel.DemurrageAgreedAmount_QBC <> 0.0
											then parcel.DemurrageAgreedAmount_QBC - isnull	(
																								parcel.DemurrageAgreedAmount_QBC - (parcel.DemurrageAgreedAmount_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)),
																								0.0
																							)
										when parcel.DemurrageClaimAmount_QBC <> 0.0
											then parcel.DemurrageClaimAmount_QBC - isnull	(
																								parcel.DemurrageClaimAmount_QBC - (parcel.DemurrageClaimAmount_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)),
																								0.0
																							)
										when parcel.DemurrageVaultEstimateAmount_QBC <> 0.0
											then parcel.DemurrageVaultEstimateAmount_QBC - isnull	(
																										parcel.DemurrageVaultEstimateAmount_QBC - (parcel.DemurrageVaultEstimateAmount_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)),
																										0.0
																									)
										else null
									end
						else null
					end															AddressCommissionApplied
				from
					Parcels parcel with (nolock)
						left join ParcelProducts parprod with (nolock)
							on parprod.QBRecId = parcel.RelatedParcelProductId
						left join Products product with (nolock)
							on product.QBRecId = parprod.RelatedProductId
						left join Warehouse.Dim_Product wproduct with (nolock)
							on wproduct.ProductAlternateKey = product.QBRecId	
						left join Warehouse.Dim_Parcel wparcel with (nolock)
							on wparcel.ParcelAlternateKey = parcel.QbRecId
						left join ParcelPorts loadparcelport with (nolock)
							on loadparcelport.QBRecId = parcel.RelatedLoadPortID
						left join ParcelBerths loadparcelberth with (nolock)
							on loadparcelberth.QBRecId = parcel.RelatedLoadBerth
						left join ParcelPorts dischargeparcelport with (nolock)
							on dischargeparcelport.QBRecId = parcel.RelatedDischPortId
						left join ParcelBerths dischargeparcelberth with (nolock)
							on dischargeparcelberth.QBRecId = parcel.RelatedDischBerth
						left join Warehouse.Dim_PortBerth loadportberth with (nolock)
							on loadportberth.PortAlternateKey = loadparcelport.RelatedPortId
								and loadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
						left join Warehouse.Dim_PortBerth dischargeportberth with (nolock)
							on dischargeportberth.PortAlternateKey = dischargeparcelport.RelatedPortId
								and dischargeportberth.BerthAlternateKey = dischargeparcelberth.RelatedBerthId
						left join Warehouse.Dim_Port loadport with (nolock)
							on loadport.PortAlternateKey = loadparcelport.RelatedPortId
						left join Warehouse.Dim_Port dischargeport with (nolock)
							on dischargeport.PortAlternateKey = dischargeparcelberth.RelatedPortId
						left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture with (nolock)
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join FullStyles fs with (nolock)
							on epostfixture.RelatedChartererFullStyle = fs.QBRecId
						left join Warehouse.Dim_Owner wo with (nolock)
							on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
						left join Warehouse.Dim_Charterer wch with (nolock)
							on wch.ChartererAlternateKey = fs.RelatedChartererParentID
						left join Warehouse.Dim_Calendar cpdate with (nolock)
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e with (nolock)
													join ParcelBerths pb with (nolock)
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf with (nolock)
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp with (nolock)
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate with (nolock)
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel with (nolock)
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join Warehouse.Dim_ProductQuantity pq with (nolock)
							on parcel.BLQty >= pq.MinimumQuantity
								and parcel.BLQty < pq.MaximumQuantity
				where
					parcel.RelatedSPIFixtureId is not null
					and coalesce	(
										parcel.DemurrageAgreedAmount_QBC,
										parcel.DemurrageClaimAmount_QBC,
										parcel.DemurrageVaultEstimateAmount_QBC,
										-1
									) >= 0;
	end try
	begin catch
		select @ErrorMsg = 'Staging Demurrage records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Get Additional Charges
	begin try
		insert
				Staging.Fact_FixtureFinance with (tablock)
			select
				distinct
					charge.RelatedSPIFixtureId									PostFixtureAlternateKey,
					-1															RebillAlternateKey,
					charge.QBRecId												ChargeAlternateKey,
					isnull(parcel.RelatedParcelProductId, -1)					ParcelProductAlternateKey,
					isnull(parprod.RelatedProductId, -1)						ProductAlternateKey,
					parcel.QbRecId												ParcelAlternateKey,
					@AdditionalChargeType										ChargeTypeAlternateKey,
					isnull(loadportberth.PortBerthKey, -1)						LoadPortBerthKey,
					isnull(dischargeportberth.PortBerthKey, -1)					DischargePortBerthKey,
					isnull(loadport.PortKey, -1)								LoadPortKey,
					isnull(dischargeport.PortKey, -1)							DischargePortKey,
					isnull(wproduct.ProductKey, -1)								ProductKey,
					isnull(wparcel.ParcelKey, -1)								ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)						PostFixtureKey,
					isnull(vessel.VesselKey, -1)								VesselKey,
					isnull(cpdate.DateKey, -1)									CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)							FirstLoadEventDateKey,
					isnull(wch.ChartererKey, -1)								ChartererKey,
					isnull(wo.OwnerKey, -1)										OwnerKey,
					isnull(pq.ProductQuantityKey, -1)							ProductFixtureQuantityKey,
					chargetype.ChargeType										ChargeType,
					charge.[Description]										ChargeDescription,
					null														ParcelNumber,
					case
						when charge.ProrationType = 'Tonnage' and parceltotals.TotalQty > 0 and parcel.BLQty > 0
							then (parcel.BLQty/convert(decimal(18, 6), parceltotals.TotalQty) * charge.Amount)
						when charge.ProrationType = 'Tonnage' and parceltotals.TotalQty > 0 and parcel.BLQty = 0 and parceltotals.ParcelCount > 0
							then (convert(decimal(18, 6), parceltotals.TotalQty)/parceltotals.ParcelCount * charge.Amount)
						when charge.ProrationType = 'Equally' and parceltotals.ParcelCount > 0
							then charge.Amount/convert(decimal(18, 6), parceltotals.ParcelCount)
						else null
					end															Charge,
					null														ChargePerMetricTon,
					try_convert	(
									decimal(20, 8),
									epostfixture.AddressCommissionPercent
								) / 100											AddressCommissionRate,
					case
						when isnull(charge.Apply_Address_Commission_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then charge.Amount * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)
						else null
					end															AddressCommissionAmount,
					case
						when isnull(charge.Apply_Address_Commission_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then charge.Amount - isnull	(
															charge.Amount - (charge.Amount * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)),
															0.0
														)
						else null
					end															AddressCommissionApplied
				from
					Parcels parcel with (nolock)
						join	(
									select
											RelatedSpiFixtureId,
											count(*) ParcelCount,
											sum(BLQty) TotalQty
										from
											Parcels with (nolock)
										group by
											RelatedSpiFixtureId
								) parceltotals
							on parceltotals.RelatedSpiFixtureId = parcel.RelatedSpiFixtureId
						left join ParcelProducts parprod with (nolock)
							on parprod.QBRecId = parcel.RelatedParcelProductId
						left join Products product with (nolock)
							on product.QBRecId = parprod.RelatedProductId
						left join Warehouse.Dim_Product wproduct with (nolock)
							on wproduct.ProductAlternateKey = product.QBRecId	
						left join Warehouse.Dim_Parcel wparcel with (nolock)
							on wparcel.ParcelAlternateKey = parcel.QbRecId
						left join ParcelPorts loadparcelport with (nolock)
							on loadparcelport.QBRecId = parcel.RelatedLoadPortID
						left join ParcelBerths loadparcelberth with (nolock)
							on loadparcelberth.QBRecId = parcel.RelatedLoadBerth
						left join ParcelPorts dischargeparcelport with (nolock)
							on dischargeparcelport.QBRecId = parcel.RelatedDischPortId
						left join ParcelBerths dischargeparcelberth with (nolock)
							on dischargeparcelberth.QBRecId = parcel.RelatedDischBerth
						left join Warehouse.Dim_PortBerth loadportberth with (nolock)
							on loadportberth.PortAlternateKey = loadparcelport.RelatedPortId
								and loadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
						left join Warehouse.Dim_PortBerth dischargeportberth with (nolock)
							on dischargeportberth.PortAlternateKey = dischargeparcelport.RelatedPortId
								and dischargeportberth.BerthAlternateKey = dischargeparcelberth.RelatedBerthId
						left join Warehouse.Dim_Port loadport with (nolock)
							on loadport.PortAlternateKey = loadparcelport.RelatedPortId
						left join Warehouse.Dim_Port dischargeport with (nolock)
							on dischargeport.PortAlternateKey = dischargeparcelberth.RelatedPortId
						left join AdditionalCharges charge with (nolock)
							on charge.RelatedSPIFixtureId = parcel.RelatedSpiFixtureId
						left join AdditionalChargeType chargetype with (nolock)
							on chargetype.RecordID = charge.RelatedAdditionalChargeType
						left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
							on wpostfixture.PostFixtureAlternateKey = charge.RelatedSPIFixtureId
						left join PostFixtures epostfixture with (nolock)
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join FullStyles fs with (nolock)
							on epostfixture.RelatedChartererFullStyle = fs.QBRecId
						left join Warehouse.Dim_Owner wo with (nolock)
							on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
						left join Warehouse.Dim_Charterer wch with (nolock)
							on wch.ChartererAlternateKey = fs.RelatedChartererParentID
						left join Warehouse.Dim_Calendar cpdate with (nolock)
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e with (nolock)
													join ParcelBerths pb with (nolock)
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf with (nolock)
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp with (nolock)
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate with (nolock)
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel with (nolock)
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join Warehouse.Dim_ProductQuantity pq with (nolock)
							on parcel.BLQty >= pq.MinimumQuantity
								and parcel.BLQty < pq.MaximumQuantity
				where
					charge.RelatedSPIFixtureId is not null
					and charge.ProrationType not in ('Individual', 'None')
					and charge.DoNotIncludeInAdditionalChargeCalculation = 0
	end try
	begin catch
		select @ErrorMsg = 'Staging AdditionalCharge records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Get Fixture-level Charges
	begin try
		insert
				Staging.Fact_FixtureFinance with (tablock)
			select
				distinct
					charge.RelatedSPIFixtureId									PostFixtureAlternateKey,
					-1															RebillAlternateKey,
					charge.QBRecId												ChargeAlternateKey,
					-1															ParcelProductAlternateKey,
					-1															ProductAlternateKey,
					-1															ParcelAlternateKey,
					@AdditionalChargeType										ChargeTypeAlternateKey,
					-1															LoadPortBerthKey,
					-1															DischargePortBerthKey,
					-1															LoadPortKey,
					-1															DischargePortKey,
					-1															ProductKey,
					-1															ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)						PostFixtureKey,
					isnull(vessel.VesselKey, -1)								VesselKey,
					isnull(cpdate.DateKey, -1)									CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)							FirstLoadEventDateKey,
					isnull(wch.ChartererKey, -1)								ChartererKey,
					isnull(wo.OwnerKey, -1)										OwnerKey,
					-1															ProductFixtureQuantityKey,
					chargetype.ChargeType										ChargeType,
					charge.[Description]										ChargeDescription,
					null														ParcelNumber,
					charge.Amount												Charge,
					null														ChargePerMetricTon,
					try_convert	(
									decimal(20, 8),
									epostfixture.AddressCommissionPercent
								) / 100											AddressCommissionRate,
					case
						when isnull(charge.Apply_Address_Commission_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then charge.Amount * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)
						else null
					end															AddressCommissionAmount,
					case
						when isnull(charge.Apply_Address_Commission_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then charge.Amount - isnull	(
															charge.Amount - (charge.Amount * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)),
															0.0
														)
						else null
					end															AddressCommissionApplied
				from
					PostFixtures epostfixture
						left join AdditionalCharges charge with (nolock)
							on charge.RelatedSPIFixtureId = epostfixture.QBRecId
						left join AdditionalChargeType chargetype with (nolock)
							on chargetype.RecordID = charge.RelatedAdditionalChargeType
						left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
							on wpostfixture.PostFixtureAlternateKey = charge.RelatedSPIFixtureId
						left join FullStyles fs with (nolock)
							on epostfixture.RelatedChartererFullStyle = fs.QBRecId
						left join Warehouse.Dim_Owner wo with (nolock)
							on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
						left join Warehouse.Dim_Charterer wch with (nolock)
							on wch.ChartererAlternateKey = fs.RelatedChartererParentID
						left join Warehouse.Dim_Calendar cpdate with (nolock)
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e with (nolock)
													join ParcelBerths pb with (nolock)
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf with (nolock)
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp with (nolock)
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate with (nolock)
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel with (nolock)
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
				where
					charge.RelatedSPIFixtureId is not null
					and charge.ProrationType = 'None'
					and charge.DoNotIncludeInAdditionalChargeCalculation = 0
	end try
	begin catch
		select @ErrorMsg = 'Staging Fixture-level records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Get Parcel Charges
	begin try
		insert
				Staging.Fact_FixtureFinance with (tablock)
			select
				distinct
					parcel.RelatedSPIFixtureId																PostFixtureAlternateKey,
					charge.RecordID																			RebillAlternateKey,
					-1																						ChargeAlternateKey,
					isnull(parcel.RelatedParcelProductId, -1)												ParcelProductAlternateKey,
					isnull(parprod.RelatedProductId, -1)													ProductAlternateKey,
					isnull(parcel.QbRecId, -1)																ParcelAlternateKey,
					@ParcelChargeType																		ChargeTypeAlternateKey,
					isnull(loadportberth.PortBerthKey, -1)													LoadPortBerthKey,
					isnull(dischargeportberth.PortBerthKey, -1)												DischargePortBerthKey,
					isnull(loadport.PortKey, -1)															LoadPortKey,
					isnull(dischargeport.PortKey, -1)														DischargePortKey,
					isnull(wproduct.ProductKey, -1)															ProductKey,
					isnull(wparcel.ParcelKey, -1)															ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)													PostFixtureKey,
					isnull(vessel.VesselKey, -1)															VesselKey,
					isnull(cpdate.DateKey, -1)																CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)														FirstLoadEventDateKey,
					isnull(wch.ChartererKey, -1)															ChartererKey,
					isnull(wo.OwnerKey, -1)																	OwnerKey,
					isnull(pq.ProductQuantityKey, -1)														ProductFixtureQuantityKey,
					chargetype.ChargeType																	ChargeType,		
					addcharges.[Description]																ChargeDescription,
					null																					ParcelNumber,
					charge.ParcelAdditionalChargeAmountDue_QBC												Charge,
					case
						when isnull(parcel.BLQty, 0) > 0
							then
								charge.ParcelAdditionalChargeAmountDue_QBC/parcel.BLQty
						else null
					end																						ChargePerMetricTon,
					try_convert	(
									decimal(20, 8),
									epostfixture.AddressCommissionPercent
								) / 100																		AddressCommissionRate,
					case
						when isnull(epostfixture.Add_Comm_applies_to_Demurrage_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then charge.ParcelAdditionalChargeAmountDue_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)
						else null
					end																						AddressCommissionAmount,
					case
						when isnull(epostfixture.Add_Comm_applies_to_Demurrage_ADMIN, 0) = 1 and isnull(try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent), 0.0) > 0.0
							then charge.ParcelAdditionalChargeAmountDue_QBC - isnull	(
																							charge.ParcelAdditionalChargeAmountDue_QBC - (charge.ParcelAdditionalChargeAmountDue_QBC * (try_convert(decimal(20, 8), epostfixture.AddressCommissionPercent) / 100)),
																							0.0
																						)
						else null
					end																						AddressCommissionApplied
				from
					ParcelAdditionalCharges charge with (nolock)
						left join AdditionalCharges addcharges with (nolock)
							on addcharges.QBRecId = charge.RelatedAdditionalChargeID
						left join AdditionalChargeType chargetype with (nolock)
							on addcharges.RelatedAdditionalChargeType = chargetype.RecordID 
						left join Parcels parcel with (nolock)
							on parcel.QbRecId = charge.RelatedParcelID						
						left join ParcelProducts parprod with (nolock)
							on parprod.QBRecId = parcel.RelatedParcelProductId
						left join Products product with (nolock)
							on product.QBRecId = parprod.RelatedProductId
						left join Warehouse.Dim_Product wproduct with (nolock)
							on wproduct.ProductAlternateKey = product.QBRecId	
						left join Warehouse.Dim_Parcel wparcel with (nolock)
							on wparcel.ParcelAlternateKey = parcel.QbRecId
						left join ParcelPorts loadparcelport with (nolock)
							on loadparcelport.QBRecId = parcel.RelatedLoadPortID
						left join ParcelBerths loadparcelberth with (nolock)
							on loadparcelberth.QBRecId = parcel.RelatedLoadBerth
						left join ParcelPorts dischargeparcelport with (nolock)
							on dischargeparcelport.QBRecId = parcel.RelatedDischPortId
						left join ParcelBerths dischargeparcelberth with (nolock)
							on dischargeparcelberth.QBRecId = parcel.RelatedDischBerth
						left join Warehouse.Dim_PortBerth loadportberth with (nolock)
							on loadportberth.PortAlternateKey = loadparcelport.RelatedPortId
								and loadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
						left join Warehouse.Dim_PortBerth dischargeportberth with (nolock)
							on dischargeportberth.PortAlternateKey = dischargeparcelport.RelatedPortId
								and dischargeportberth.BerthAlternateKey = dischargeparcelberth.RelatedBerthId
						left join Warehouse.Dim_Port loadport with (nolock)
							on loadport.PortAlternateKey = loadparcelport.RelatedPortId
						left join Warehouse.Dim_Port dischargeport with (nolock)
							on dischargeport.PortAlternateKey = dischargeparcelberth.RelatedPortId
						left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture with (nolock)
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join FullStyles fs with (nolock)
							on epostfixture.RelatedChartererFullStyle = fs.QBRecId
						left join Warehouse.Dim_Owner wo with (nolock)
							on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
						left join Warehouse.Dim_Charterer wch with (nolock)
							on wch.ChartererAlternateKey = fs.RelatedChartererParentID
						left join Warehouse.Dim_Calendar cpdate with (nolock)
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e with (nolock)
													join ParcelBerths pb with (nolock)
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf with (nolock)
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp with (nolock)
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate with (nolock)
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel with (nolock)
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join Warehouse.Dim_ProductQuantity pq with (nolock)
							on parcel.BLQty >= pq.MinimumQuantity
								and parcel.BLQty < pq.MaximumQuantity
				where
					parcel.RelatedSPIFixtureId is not null
					and parcel.RelatedParcelProductId is not null
					and addcharges.ProrationType = 'Individual';
	end try
	begin catch
		select @ErrorMsg = 'Staging ParcelAdditionalCharges records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Update ParcelNumber
	begin try
		update
				Staging.Fact_FixtureFinance with (tablock)
			set
				ParcelNumber = parcelnumbers.ParcelNumber
			from
				(
					select
							row_number() over (partition by p.RelatedSpiFixtureId order by p.QbRecId)	ParcelNumber,
							p.RelatedSpiFixtureId,
							p.QbRecId ParcelId
						from
							Parcels p with (tablock)
				) parcelnumbers
			where
				parcelnumbers.ParcelId = Staging.Fact_FixtureFinance.ParcelAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating ParcelNumber - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_FixtureFinance', 'U') is not null
		truncate table Warehouse.Fact_FixtureFinance;

	-- Insert new charges into Warehouse table
	begin try
		insert
				Warehouse.Fact_FixtureFinance with (tablock)
			select
					finance.PostFixtureAlternateKey,
					finance.RebillAlternateKey,
					finance.ChargeAlternateKey,
					finance.ParcelProductAlternateKey,
					finance.ProductAlternateKey,
					finance.ParcelAlternateKey,
					finance.ChargeTypeAlternateKey,
					finance.LoadPortBerthKey,
					finance.DischargePortBerthKey,
					finance.LoadPortKey,
					finance.DischargePortKey,
					finance.ProductKey,
					finance.ParcelKey,
					finance.PostFixtureKey,
					finance.VesselKey,
					finance.CharterPartyDateKey,
					finance.FirstLoadEventDateKey,
					finance.ChartererKey,
					finance.OwnerKey,
					finance.ProductFixtureQuantityKey,
					finance.ChargeType,
					finance.ChargeDescription,
					finance.ParcelNumber,
					finance.Charge,
					finance.ChargePerMetricTon,
					finance.AddressCommissionRate,
					finance.AddressCommissionAmount,
					finance.AddressCommissionApplied,
					getdate() RowStartDate
				from
					Staging.Fact_FixtureFinance finance  with (tablock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end