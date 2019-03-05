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

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_FixtureFinance', 'U') is not null
		truncate table Staging.Fact_FixtureFinance;

	-- Get Additional Charges
	begin try
		insert
				Staging.Fact_FixtureFinance
			select
				distinct
					charge.RelatedSPIFixtureId					PostFixtureAlternateKey,
					-1											RebillAlternateKey,
					charge.QBRecId								ChargeAlternateKey,
					-1											ParcelProductAlternateKey,
					-1											ProductAlternateKey,
					-1											LoadPortKey,
					-1											LoadBerthKey,
					-1											DischargePortKey,
					-1											DischargeBerthKey,
					-1											ProductKey,
					-2											ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
					isnull(vessel.VesselKey, -1)				VesselKey,
					charge.[Type]								ChargeType,
					charge.[Description]						ChargeDescription,
					null										ParcelNumber,
					charge.Amount								Charge,
					null										ChargePerMetricTon,
					isnull(rs.RecordStatus, @NewRecord)			RecordStatus
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
												RebillAlternateKey,
												ChargeAlternateKey,
												ParcelProductAlternateKey,
												ProductAlternateKey
											from
												Warehouse.Fact_FixtureFinance
									) rs
							on rs.PostFixtureAlternateKey = charge.RelatedSPIFixtureId
								and rs.RebillAlternateKey = -1
								and rs.ChargeAlternateKey = charge.QBRecId
								and rs.ParcelProductAlternateKey = -1
								and rs.ProductAlternateKey = -1
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
				Staging.Fact_FixtureFinance
			select
				distinct
					parcel.RelatedSPIFixtureId					PostFixtureAlternateKey,
					charge.RecordID								RebillAlternateKey,
					-1											ChargeAlternateKey,
					parcel.RelatedParcelProductId				ParcelProductAlternateKey,
					isnull(parprod.RelatedProductId, -1)		ProductAlternateKey,
					isnull(loadport.PortKey, -1)				LoadPortKey,
					isnull(loadberth.BerthKey, -1)				LoadBerthKey,
					isnull(dischargeport.PortKey, -1)			DischargePortKey,
					isnull(dischargeberth.BerthKey, -1)			DischargeBerthKey,
					isnull(wproduct.ProductKey, -1)				ProductKey,
					isnull(wparcel.ParcelKey, -1)				ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
					isnull(vessel.VesselKey, -1)				VesselKey,
					chargetype.[Type]							ChargeType,		
					chargetype.[Description]					ChargeDescription,
					null										ParcelNumber,
					charge.ParcelAdditionalChargeAmountDue_QBC	Charge,
					case
						when isnull(parcel.BLQty, 0) > 0
							then
								charge.ParcelAdditionalChargeAmountDue_QBC/parcel.BLQty
						else null
					end											ChargePerMetricTon,
					isnull(rs.RecordStatus, @NewRecord)			RecordStatus
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
						left join Warehouse.Dim_Port loadport
							on loadport.PortAlternateKey = parcel.RelatedLoadPortID
						left join Warehouse.Dim_Port dischargeport
							on dischargeport.PortAlternateKey = parcel.RelatedDischPortId
						left join Warehouse.Dim_Berth loadberth
							on loadberth.BerthAlternateKey = parcel.RelatedLoadBerth
						left join Warehouse.Dim_Berth dischargeberth
							on dischargeberth.BerthAlternateKey = parcel.RelatedDischBerth
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join	(
										select
												@ExistingRecord RecordStatus,
												PostFixtureAlternateKey,
												RebillAlternateKey,
												ChargeAlternateKey,
												ParcelProductAlternateKey,
												ProductAlternateKey
											from
												Warehouse.Fact_FixtureFinance
									) rs
							on rs.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
								and rs.RebillAlternateKey = charge.RecordID
								and rs.ChargeAlternateKey = -1
								and rs.ParcelProductAlternateKey = parcel.RelatedParcelProductId
								and rs.ProductAlternateKey = parprod.RelatedProductId
				where
					parcel.RelatedSPIFixtureId is not null
					and parcel.RelatedParcelProductId is not null;
	end try
	begin catch
		select @ErrorMsg = 'Staging ParcelAdditionalCharges records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	

	-- Get Freight Charges
	-- Get Demurrage Charges
	begin try
		insert
				Staging.Fact_FixtureFinance
			select
				distinct
					parcel.RelatedSPIFixtureId					PostFixtureAlternateKey,
					-1											RebillAlternateKey,
					-1											ChargeAlternateKey,
					parcel.QbRecId								ParcelProductAlternateKey,
					isnull(parprod.RelatedProductId, -1)		ProductAlternateKey,
					isnull(loadport.PortKey, -1)				LoadPortKey,
					isnull(loadberth.BerthKey, -1)				LoadBerthKey,
					isnull(dischargeport.PortKey, -1)			DischargePortKey,
					isnull(dischargeberth.BerthKey, -1)			DischargeBerthKey,
					isnull(wproduct.ProductKey, -1)				ProductKey,
					isnull(wparcel.ParcelKey, -1)				ParcelKey,
					isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
					isnull(vessel.VesselKey, -1)				VesselKey,
					'Demurrage'									ChargeType,		
					null										ChargeDescription,
					null										ParcelNumber,
					parcel.ParcelDemurrageAmount_QBC			Charge,
					case
						when isnull(parcel.BLQty, 0) > 0
							then
								parcel.ParcelDemurrageAmount_QBC/parcel.BLQty
						else null
					end											ChargePerMetricTon,
					isnull(rs.RecordStatus, @NewRecord)			RecordStatus
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
						left join Warehouse.Dim_Port loadport
							on loadport.PortAlternateKey = parcel.RelatedLoadPortID
						left join Warehouse.Dim_Port dischargeport
							on dischargeport.PortAlternateKey = parcel.RelatedDischPortId
						left join Warehouse.Dim_Berth loadberth
							on loadberth.BerthAlternateKey = parcel.RelatedLoadBerth
						left join Warehouse.Dim_Berth dischargeberth
							on dischargeberth.BerthAlternateKey = parcel.RelatedDischBerth
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join	(
										select
												@ExistingRecord RecordStatus,
												PostFixtureAlternateKey,
												RebillAlternateKey,
												ChargeAlternateKey,
												ParcelProductAlternateKey,
												ProductAlternateKey
											from
												Warehouse.Fact_FixtureFinance
									) rs
							on rs.PostFixtureAlternateKey = parcel.RelatedSPIFixtureId
								and rs.RebillAlternateKey = -1
								and rs.ChargeAlternateKey = -1
								and rs.ParcelProductAlternateKey = parcel.RelatedParcelProductId
								and rs.ProductAlternateKey = parprod.RelatedProductId
				where
					parcel.RelatedSPIFixtureId is not null
					and parcel.ParcelDemurrageAmount_QBC is not null;
	end try
	begin catch
		select @ErrorMsg = 'Staging Demurrage records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	


	---- Update LoadDischarge
	--begin try
	--	update
	--			Staging.Fact_FixtureFinance
	--		set
	--			LoadDischarge = pp.[Type]
	--		from
	--			ParcelPorts pp
	--		where
	--			pp.QBRecId = Staging.Fact_FixtureFinance.ParcelPortAlternateKey;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Updating LoadDischarge - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	---- Update ProrationPercentage
	--begin try
	--	update
	--			Staging.Fact_FixtureFinance
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
	--			Staging.Fact_FixtureFinance
	--		set
	--			StartDate = datetimefromparts(year(StartDate), month(StartDate), day(StartDate), datepart(hour, StartTime), datepart(minute, StartTime), 0, 0),
	--			StopDate = datetimefromparts(year(StopDate), month(StopDate), day(StopDate), datepart(hour, StopTime), datepart(minute, StopTime), 0, 0);
	
	--	-- Calculate Duration
	--	update
	--			Staging.Fact_FixtureFinance
	--		set
	--			Duration =	case
	--							when StartDateKey > 19000000 and StopDateKey < 47000000
	--								then datediff(minute, StartDate, StopDate)/60.0
	--							else null
	--						end;

	--	-- Calculate LaytimeActual
	--	update
	--			Staging.Fact_FixtureFinance
	--		set
	--			LaytimeActual =	case IsLaytime
	--								when 'Y'
	--									then ProrationPercentage*Duration
	--								else null
	--							end;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Updating FixtureFinance Duration/LaytimeActual - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	---- Update ParcelNumber
	--begin try
	--	update
	--			Staging.Fact_FixtureFinance
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
	--			parcelnumbers.ParcelId = Staging.Fact_FixtureFinance.ParcelAlternateKey;
	--end try
	--begin catch
	--	select @ErrorMsg = 'Updating ParcelNumber - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch	

	-- Insert new charges into Warehouse table
	begin try
		insert
				Warehouse.Fact_FixtureFinance
			select
					finance.PostFixtureAlternateKey,
					finance.RebillAlternateKey,
					finance.ChargeAlternateKey,
					finance.ParcelProductAlternateKey,
					finance.ProductAlternateKey,
					finance.LoadPortKey,
					finance.LoadBerthKey,
					finance.DischargePortKey,
					finance.DischargeBerthKey,
					finance.ProductKey,
					finance.ParcelKey,
					finance.PostFixtureKey,
					finance.VesselKey,
					finance.ChargeType,
					finance.ChargeDescription,
					finance.ParcelNumber,
					finance.Charge,
					finance.ChargePerMetricTon,
					getdate() RowStartDate,
					getdate() RowUpdatedDate
				from
					Staging.Fact_FixtureFinance finance
				where
					finance.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end