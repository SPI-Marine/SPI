/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/23/2020
Description:	Creates the Staging.Fact_OwnerInvoice table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	07/29/2020	Added InvoiceStatus
Brian Boswick	07/12/2021	Removed COAKey
==========================================================================================================	
*/

drop table if exists Staging.Fact_OwnerInvoice;
go

create table Staging.Fact_OwnerInvoice
	(
		OwnerInvoiceAlternateKey			int					not null,
		OwnerInvoiceDateKey					int					not null,
		OwnersReceivedFrtPaymentDateKey		int					not null,
		InvoiceReceivedBySPIDateKey			int					not null,
		ValidatedBySPIDateKey				int					not null,
		InvoiceSentToChartererDateKey		int					not null,
		VerifiedByChartererDateKey			int					not null,
		PostFixtureKey						int					not null,
		OwnerInvoiceNumber					varchar(150)		null,		-- Degenerate Dimension Attributes
		OwnerInvoiceAttachment				varchar(5000)		null,
		Currency							varchar(50)			null,
		InvoiceType							varchar(50)			null,
		InternalInvoiceNotes				varchar(5000)		null,
		VerifiedByChartererDate				date				null,
		ValidatedBySPIDate					date				null,
		InvoiceReceivedBySPIDate			date				null,
		InvoiceSentToChartererDate			date				null,
		InvoiceStatus						varchar(100)		null,
		InvoiceDueAmount					decimal(20, 8)		null,		-- Metrics
		constraint [PK_Staging_Fact_OwnerInvoice_AltKey] primary key clustered 
		(
			OwnerInvoiceAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];