/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/23/2020
Description:	Creates the Warehouse.Fact_OwnerInvoice table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	07/29/2020	Added COAKey
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_OwnerInvoice;
go

create table Warehouse.Fact_OwnerInvoice
	(
		OwnerInvoiceKey						int					not null identity(1, 1),
		OwnerInvoiceAlternateKey			int					not null,
		OwnerInvoiceDateKey					int					not null,
		OwnersReceivedFrtPaymentDateKey		int					not null,
		InvoiceReceivedBySPIDateKey			int					not null,
		ValidatedBySPIDateKey				int					not null,
		InvoiceSentToChartererDateKey		int					not null,
		VerifiedByChartererDateKey			int					not null,
		PostFixtureKey						int					not null,
		COAKey								int					not null,
		OwnerInvoiceNumber					varchar(150)		null,		-- Degenerate Dimension Attributes
		OwnerInvoiceAttachment				varchar(5000)		null,
		Currency							varchar(50)			null,
		InvoiceType							varchar(50)			null,
		InternalInvoiceNotes				varchar(5000)		null,
		VerifiedByChartererDate				date				null,
		ValidatedBySPIDate					date				null,
		InvoiceReceivedBySPIDate			date				null,
		InvoiceSentToChartererDate			date				null,
		InvoiceDueAmount					decimal(20, 8)		null,		-- Metrics
		RowCreatedDate						datetime			not null,	-- ETL fields
		constraint [PK_Warehouse_Fact_OwnerInvoice_OwnerInvoiceKey] primary key clustered 
		(
			OwnerInvoiceKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];