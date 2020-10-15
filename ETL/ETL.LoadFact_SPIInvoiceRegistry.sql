set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_SPIInvoiceRegistry;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/24/2020
Description:	Creates the LoadFact_SPIInvoiceRegistry stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	08/27/2020	Added InvoiceTypeCategory
==========================================================================================================	
*/

create procedure ETL.LoadFact_SPIInvoiceRegistry
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_SPIInvoiceRegistry', 'U') is not null
		truncate table Staging.Fact_SPIInvoiceRegistry;

	begin try
		with MaxProducts	(
								PostFixtureAlternateKey,
								ProductAlternateKey,
								PortAlternateKey,
								LoadDischarge,
								Qty
							)
		as
		(
			select
				distinct
					parcel.RelatedSpiFixtureId			PostFixtureAlternateKey,
					max(product.RelatedProductId)		ProductAlternateKey,
					max(pp.RelatedPortId)				PortAlternateKey,
					pp.[Type]							LoadDischarge,
					parcel.BLQty						Qty
				from
					Parcels parcel with (nolock)
						join	(
									select
											p.RelatedSpiFixtureId,
											max(p.BLQty) BLQty
										from
											Parcels p with (nolock)
												join ParcelBerths pb
													on pb.QBRecId = p.RelatedLoadBerth
										group by
											p.RelatedSpiFixtureId
								) maxqty
							on maxqty.RelatedSpiFixtureId = parcel.RelatedSpiFixtureId
								and maxqty.BLQty = parcel.BLQty
						join ParcelBerths pb with (nolock)
							on parcel.RelatedLoadBerth = pb.QBRecId
						join ParcelPorts pp with (nolock)
							on pb.RelatedLDPId = pp.QBRecId
						join ParcelProducts product
							on product.QBRecId = parcel.RelatedParcelProductId
				group by
					parcel.RelatedSpiFixtureId,
					pp.[Type],
					parcel.BLQty
			union
			select
				distinct
					parcel.RelatedSpiFixtureId			PostFixtureAlternateKey,
					max(product.RelatedProductId)		ProductAlternateKey,
					max(pp.RelatedPortId)				PortAlternateKey,
					pp.[Type]							LoadDischarge,
					parcel.BLQty						Qty
				from
					Parcels parcel with (nolock)
						join	(
									select
											p.RelatedSpiFixtureId,
											max(p.BLQty) BLQty
										from
											Parcels p with (nolock)
												join ParcelBerths pb
													on pb.QBRecId = p.RelatedDischBerth
										group by
											p.RelatedSpiFixtureId
								) maxqty
							on maxqty.RelatedSpiFixtureId = parcel.RelatedSpiFixtureId
								and maxqty.BLQty = parcel.BLQty
						join ParcelBerths pb with (nolock)
							on parcel.RelatedDischBerth = pb.QBRecId
						join ParcelPorts pp with (nolock)
							on pb.RelatedLDPId = pp.QBRecId
						join ParcelProducts product
							on product.QBRecId = parcel.RelatedParcelProductId
				group by
					parcel.RelatedSpiFixtureId,
					pp.[Type],
					parcel.BLQty
		)

		insert
				Staging.Fact_SPIInvoiceRegistry with (tablock)
		select
			distinct
				ir.RecordID											InvoiceAlternateKey,
				isnull(invdate.DateKey, 18991230)					InvoiceDateKey,
				isnull(invduedate.DateKey, 18991230)				InvoiceDueDateKey,
				isnull(pmtrecdate.DateKey, 18991230)				PaymentReceivedDateKey,
				isnull(pf.PostFixtureKey, -1)						PostFixtureKey,
				isnull(loadport.PortKey, -1)						LoadPortKey,
				isnull(dischport.PortKey, -1)						DischargePortKey,
				isnull(product.ProductKey, -1)						ProductKey,
				-1													OwnerKey,
				-1													ChartererKey,
				isnull(pq.ProductQuantityKey, -1)					ProductQuantityKey,
				isnull(cpdate.DateKey, 18991230)					CPDateKey,
				isnull(coa.COAKey, -1)								COAKey,
				isnull(tc.TimeChartererKey, -1)						TimeChartererKey,
				ir.InvoiceNumberOfficial_INVOICE					InvoiceNumber,
				ir.InvoiceType_ADMIN								InvoiceType,
				ir.InvoiceTo_INVOICE								InvoiceTo,
				ir.InvoiceStatus_ADMIN								InvoiceStatus,
				ir.VesselFormula_INVOICE							VesselFormula,
				ir.OfficeFormula_ADMIN								OfficeFormula,
				ir.SPIRegion										OfficeFormula,
				ir.BrokerFormula_ADMIN								BrokerFormula,
				ir.ChartererFormula_INVOICE							ChartererFormula,
				ir.OwnerFormula_INVOICE								OwnerFormula,
				ir.InvoiceGeneratedBy_ADMIN							InvoiceGeneratedBy,
				ir.CreditAppliedAgainstInvoiceNumber				CreditAppliedAgainstInvoiceNumber,
				ir.Currency_INVOICE									CurrencyInvoice,
				ir.InvoiceTypeCategory_ADMIN						InvoiceTypeCategory,
				replace(ir.InvoiceAmountSnapShot_ADMIN, ',', '')	InvoiceAmount
			from
				InvoiceRegistry ir with (nolock)
					left join Warehouse.Dim_TimeCharterer tc (nolock)
						on tc.TimeChartererAlternateKey = ir.RelatedSPITimeChartererID
					left join Warehouse.Dim_PostFixture pf with (nolock)
						on pf.PostFixtureAlternateKey = ir.RelatedSPIFixtureID
					left join PostFixtures epf with (nolock)
						on pf.PostFixtureAlternateKey = epf.QBRecId
					left join Warehouse.Dim_COA coa (nolock)
						on coa.COAAlternateKey = epf.RelatedSPICOAId
					left join MaxProducts loadproduct with (nolock)
						on loadproduct.PostFixtureAlternateKey = ir.RelatedSPIFixtureID
							and loadproduct.LoadDischarge = 'Load'
					left join Warehouse.Dim_Port loadport with (nolock)
						on loadport.PortAlternateKey = loadproduct.PortAlternateKey
					left join MaxProducts dischproduct with (nolock)
						on dischproduct.PostFixtureAlternateKey = ir.RelatedSPIFixtureID
							and dischproduct.LoadDischarge = 'Discharge'
					left join Warehouse.Dim_Port dischport with (nolock)
						on dischport.PortAlternateKey = dischproduct.PortAlternateKey
					left join FullStyles cfs with (nolock)
						on epf.RelatedChartererFullStyle = cfs.QBRecId
					left join Warehouse.Dim_Calendar cpdate with (nolock)
						on cpdate.FullDate = convert(date, pf.CPDate)
					left join Warehouse.Dim_Calendar invdate with (nolock)
						on invdate.FullDate = convert(date, ir.InvoiceDateINVOICE)
					left join Warehouse.Dim_Calendar pmtrecdate with (nolock)
						on pmtrecdate.FullDate = convert(date, ir.DatePaymentReceived)
					left join Warehouse.Dim_Calendar invduedate with (nolock)
						on invduedate.FullDate = convert(date, ir.InvoiceDueDate)
					left join Warehouse.Dim_Product product with (nolock)
						on product.ProductAlternateKey = loadproduct.ProductAlternateKey
					left join Warehouse.Dim_ProductQuantity pq with (nolock)
						on loadproduct.Qty between pq.MinimumQuantity and pq.MaximumQuantity
			where
				ir.InvoiceNumberOfficial_INVOICE not like '%draft%'
				and isnull(cast(replace(ir.InvoiceAmountSnapShot_ADMIN, ',', '') as numeric(20, 6)), 0.0) <> 0.0
	end try
	begin catch
		select @ErrorMsg = 'Staging Invoice records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_SPIInvoiceRegistry', 'U') is not null
		truncate table Warehouse.Fact_SPIInvoiceRegistry;

	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_SPIInvoiceRegistry with (tablock)
			select
					inv.InvoiceAlternateKey,
					inv.InvoiceDateKey,
					inv.InvoiceDueDateKey,
					inv.PaymentReceivedDateKey,
					inv.PostFixtureKey,
					inv.LoadPortKey,
					inv.DischargePortKey,
					inv.ProductKey,
					inv.OwnerKey,
					inv.ChartererKey,
					inv.ProductQuantityKey,
					inv.CPDateKey,
					inv.COAKey,
					inv.TimeChartererKey,
					inv.InvoiceNumber,
					inv.InvoiceType,
					inv.InvoiceTo,
					inv.InvoiceStatus,
					inv.VesselFormula,
					inv.OfficeFormula,
					inv.RegionFormula,
					inv.BrokerFormula,
					inv.ChartererFormula,
					inv.OwnerFormula,
					inv.InvoiceGeneratedBy,
					inv.CreditAppliedAgainstInvoiceNumber,
					inv.CurrencyInvoice,
					inv.InvoiceTypeCategory,
					inv.InvoiceAmount,
					getdate() RowStartDate
				from
					Staging.Fact_SPIInvoiceRegistry inv;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end