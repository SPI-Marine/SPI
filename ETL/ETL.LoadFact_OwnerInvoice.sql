set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_OwnerInvoice;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/23/2020
Description:	Creates the LoadFact_OwnerInvoice stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_OwnerInvoice
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_OwnerInvoice', 'U') is not null
		truncate table Staging.Fact_OwnerInvoice;

	begin try
		insert
				Staging.Fact_OwnerInvoice with (tablock)	(	
																OwnerInvoiceAlternateKey,
																OwnerInvoiceDateKey,
																OwnersReceivedFrtPaymentDateKey,
																InvoiceReceivedBySPIDateKey,
																ValidatedBySPIDateKey,
																InvoiceSentToChartererDateKey,
																VerifiedByChartererDateKey,
																PostFixtureKey,
																OwnerInvoiceNumber,
																OwnerInvoiceAttachment,
																Currency,
																InvoiceType,
																InternalInvoiceNotes,
																InvoiceDueAmount
															)	
		select
				oi.RecordID							OwnerInvoiceAlternateKey,
				isnull(oid.DateKey, 47001231)		OwnerInvoiceDateKey,
				isnull(orfp.DateKey, 47001231)		OwnersReceivedFrtPaymentDateKey,
				isnull(irbs.DateKey, 47001231)		InvoiceReceivedBySPIDateKey,
				isnull(vbs.DateKey, 47001231)		ValidatedBySPIDateKey,
				isnull(istc.DateKey, 47001231)		InvoiceSentToChartererDateKey,
				isnull(vbc.DateKey, 47001231)		VerifiedByChartererDateKey,
				isnull(pf.PostFixtureKey, -1)		PostFixtureKey,
				oi.OwnerInvoiceNumber				OwnerInvoiceNumber,
				oi.OwnerInvoiceAttachment			OwnerInvoiceAttachment,
				oi.Currency							Currency,
				oi.InvoiceType						InvoiceType,
				oi.InternalInvoiceNotes				InternalInvoiceNotes,
				oi.InvoiceDueAmount					InvoiceDueAmount
			from
				OwnerInvoice oi with (nolock)
					left join Warehouse.Dim_PostFixture pf
						on pf.PostFixtureAlternateKey = oi.RelatedSPIFixtureID
					left join Warehouse.Dim_Calendar oid with (nolock)
						on convert(date, oi.OwnerInvoiceDate) = oid.FullDate
					left join Warehouse.Dim_Calendar orfp with (nolock)
						on convert(date, oi.DateOwnersReceivedFrtPayment) = orfp.FullDate
					left join Warehouse.Dim_Calendar istc with (nolock)
						on convert(date, oi.DateInvoiceSenttoCharterer) = istc.FullDate
					left join Warehouse.Dim_Calendar irbs with (nolock)
						on convert(date, oi.DateInvoiceReceivedbySPI) = irbs.FullDate
					left join Warehouse.Dim_Calendar vbs with (nolock)
						on convert(date, oi.DateValidatedbySPI) = vbs.FullDate
					left join Warehouse.Dim_Calendar vbc with (nolock)
						on convert(date, oi.DateVerifiedbyCharterer) = vbc.FullDate;
	end try
	begin catch
		select @ErrorMsg = 'Staging records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_OwnerInvoice', 'U') is not null
		truncate table Warehouse.Fact_OwnerInvoice;

	-- Insert records into Warehouse table
	begin try
		insert
				Warehouse.Fact_OwnerInvoice with (tablock)	(
																OwnerInvoiceAlternateKey,
																OwnerInvoiceDateKey,
																OwnersReceivedFrtPaymentDateKey,
																InvoiceReceivedBySPIDateKey,
																ValidatedBySPIDateKey,
																InvoiceSentToChartererDateKey,
																VerifiedByChartererDateKey,
																PostFixtureKey,
																OwnerInvoiceNumber,
																OwnerInvoiceAttachment,
																Currency,
																InvoiceType,
																InternalInvoiceNotes,
																InvoiceDueAmount,
																RowCreatedDate
															)
			select
					soi.OwnerInvoiceAlternateKey,
					soi.OwnerInvoiceDateKey,
					soi.OwnersReceivedFrtPaymentDateKey,
					soi.InvoiceReceivedBySPIDateKey,
					soi.ValidatedBySPIDateKey,
					soi.InvoiceSentToChartererDateKey,
					soi.VerifiedByChartererDateKey,
					soi.PostFixtureKey,
					soi.OwnerInvoiceNumber,
					soi.OwnerInvoiceAttachment,
					soi.Currency,
					soi.InvoiceType,
					soi.InternalInvoiceNotes,
					soi.InvoiceDueAmount,
					getdate()
				from
					Staging.Fact_OwnerInvoice soi;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end