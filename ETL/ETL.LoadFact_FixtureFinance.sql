/*
==========================================================================================================
Author:			Brian Boswick
Create date:	01/08/2019
Description:	Creates the LoadFact_FixtureFinance stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_FixtureFinance;
go

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
					isnull(wproduct.ProductKey, -1)								ProductKey,
					isnull(wparcel.ParcelKey, -1)								ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)						PostFixtureKey,
					isnull(vessel.VesselKey, -1)								VesselKey,
					isnull(cpdate.DateKey, -1)									CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)							FirstLoadEventDateKey,
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
					Parcels parcel
						left join ParcelProducts parprod
							on parprod.QBRecId = parcel.RelatedParcelProductId
						left join Products product
							on product.QBRecId = parprod.RelatedProductId
						left join Warehouse.Dim_Product wproduct
							on wproduct.ProductAlternateKey = product.QBRecId	
						left join Warehouse.Dim_Parcel wparcel
							on wparcel.ParcelAlternateKey = parcel.QbRecId
						left join ParcelPorts loadparcelport
							on loadparcelport.QBRecId = parcel.RelatedLoadPortID
						left join ParcelBerths loadparcelberth
							on loadparcelberth.QBRecId = parcel.RelatedLoadBerth
						left join ParcelPorts dischargeparcelport
							on dischargeparcelport.QBRecId = parcel.RelatedDischPortId
						left join ParcelBerths dischargeparcelberth
							on dischargeparcelberth.QBRecId = parcel.RelatedDischBerth
						left join Warehouse.Dim_PortBerth loadportberth
							on loadportberth.PortAlternateKey = loadparcelport.RelatedPortId
								and loadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
						left join Warehouse.Dim_PortBerth dischargeportberth
							on dischargeportberth.PortAlternateKey = dischargeparcelport.RelatedPortId
								and dischargeportberth.BerthAlternateKey = dischargeparcelberth.RelatedBerthId
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Calendar cpdate
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e
													join ParcelBerths pb
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
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
					isnull(wproduct.ProductKey, -1)								ProductKey,
					isnull(wparcel.ParcelKey, -1)								ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)						PostFixtureKey,
					isnull(vessel.VesselKey, -1)								VesselKey,
					isnull(cpdate.DateKey, -1)									CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)							FirstLoadEventDateKey,
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
					Parcels parcel
						left join ParcelProducts parprod
							on parprod.QBRecId = parcel.RelatedParcelProductId
						left join Products product
							on product.QBRecId = parprod.RelatedProductId
						left join Warehouse.Dim_Product wproduct
							on wproduct.ProductAlternateKey = product.QBRecId	
						left join Warehouse.Dim_Parcel wparcel
							on wparcel.ParcelAlternateKey = parcel.QbRecId
						left join ParcelPorts loadparcelport
							on loadparcelport.QBRecId = parcel.RelatedLoadPortID
						left join ParcelBerths loadparcelberth
							on loadparcelberth.QBRecId = parcel.RelatedLoadBerth
						left join ParcelPorts dischargeparcelport
							on dischargeparcelport.QBRecId = parcel.RelatedDischPortId
						left join ParcelBerths dischargeparcelberth
							on dischargeparcelberth.QBRecId = parcel.RelatedDischBerth
						left join Warehouse.Dim_PortBerth loadportberth
							on loadportberth.PortAlternateKey = loadparcelport.RelatedPortId
								and loadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
						left join Warehouse.Dim_PortBerth dischargeportberth
							on dischargeportberth.PortAlternateKey = dischargeparcelport.RelatedPortId
								and dischargeportberth.BerthAlternateKey = dischargeparcelberth.RelatedBerthId
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Calendar cpdate
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e
													join ParcelBerths pb
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
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
					-1															ParcelProductAlternateKey,
					-1															ProductAlternateKey,
					-1															ParcelAlternateKey,
					@AdditionalChargeType										ChargeTypeAlternateKey,
					-1															LoadPortBerthKey,
					-1															DischargePortBerthKey,
					-1															ProductKey,
					-2															ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)						PostFixtureKey,
					isnull(vessel.VesselKey, -1)								VesselKey,
					isnull(cpdate.DateKey, -1)									CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)							FirstLoadEventDateKey,
					charge.[Type]												ChargeType,
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
					AdditionalCharges charge
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = charge.RelatedSPIFixtureId
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Calendar cpdate
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e
													join ParcelBerths pb
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
				where
					charge.RelatedSPIFixtureId is not null;
	end try
	begin catch
		select @ErrorMsg = 'Staging AdditionalCharge records - ' + error_message();
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
					isnull(wproduct.ProductKey, -1)															ProductKey,
					isnull(wparcel.ParcelKey, -1)															ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)													PostFixtureKey,
					isnull(vessel.VesselKey, -1)															VesselKey,
					isnull(cpdate.DateKey, -1)																CharterPartyDateKey,
					isnull(firsteventdate.DateKey, -1)														FirstLoadEventDateKey,
					chargetype.[Type]																		ChargeType,		
					chargetype.[Description]																ChargeDescription,
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
					ParcelAdditionalCharges charge
						left join AdditionalCharges chargetype
							on chargetype.QBRecId = charge.RelatedAdditionalChargeID
						left join Parcels parcel
							on parcel.QbRecId = charge.RelatedParcelID						
						left join ParcelProducts parprod
							on parprod.QBRecId = parcel.RelatedParcelProductId
						left join Products product
							on product.QBRecId = parprod.RelatedProductId
						left join Warehouse.Dim_Product wproduct
							on wproduct.ProductAlternateKey = product.QBRecId	
						left join Warehouse.Dim_Parcel wparcel
							on wparcel.ParcelAlternateKey = parcel.QbRecId
						left join ParcelPorts loadparcelport
							on loadparcelport.QBRecId = parcel.RelatedLoadPortID
						left join ParcelBerths loadparcelberth
							on loadparcelberth.QBRecId = parcel.RelatedLoadBerth
						left join ParcelPorts dischargeparcelport
							on dischargeparcelport.QBRecId = parcel.RelatedDischPortId
						left join ParcelBerths dischargeparcelberth
							on dischargeparcelberth.QBRecId = parcel.RelatedDischBerth
						left join Warehouse.Dim_PortBerth loadportberth
							on loadportberth.PortAlternateKey = loadparcelport.RelatedPortId
								and loadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
						left join Warehouse.Dim_PortBerth dischargeportberth
							on dischargeportberth.PortAlternateKey = dischargeparcelport.RelatedPortId
								and dischargeportberth.BerthAlternateKey = dischargeparcelberth.RelatedBerthId
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Calendar cpdate
							on cpdate.FullDate = convert(date, epostfixture.CPDate)
						left join	(
										select
												pf.QBRecId			PostFixtureAlternateKey,
												min(e.StartDate)	FirstEventDate
											from
												SOFEvents e
													join ParcelBerths pb
														on pb.QBRecId = e.RelatedParcelBerthId
													join PostFixtures pf
														on pf.QBRecId = pb.RelatedSpiFixtureId
													join ParcelPorts pp
														on pb.RelatedLDPId = pp.QBRecID
													where
														pp.[Type] = 'Load'
													group by
														pf.QBRecId
									) firstevent
							on firstevent.PostFixtureAlternateKey = epostfixture.QBRecId
						left join Warehouse.Dim_Calendar firsteventdate
							on firsteventdate.FullDate = convert(date, firstevent.FirstEventDate)
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
				where
					parcel.RelatedSPIFixtureId is not null
					and parcel.RelatedParcelProductId is not null
					and not exists	(
										select
												1
											from
												Staging.Fact_FixtureFinance sff
											where
												sff.PostFixtureAlternateKey = parcel.RelatedSpiFixtureId
									);
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
							Parcels p
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
					finance.ProductKey,
					finance.ParcelKey,
					finance.PostFixtureKey,
					finance.VesselKey,
					finance.CharterPartyDateKey,
					finance.FirstLoadEventDateKey,
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
					Staging.Fact_FixtureFinance finance;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end