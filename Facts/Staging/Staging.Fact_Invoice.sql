/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/24/2020
Description:	Creates the Staging.Fact_Invoice table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_Invoice;
go

create table Staging.Fact_Invoice
	(
		InvoiceAlternateKey										int					not null,
		InvoiceDateKey											int					not null,
		InvoiceDueDateKey										int					not null,
		PaymentReceivedDateKey									int					not null,
		PostFixtureKey											int					not null,
		LoadPortKey												int					not null,
		DischargePortKey										int					not null,
		ProductKey												int					not null,
		OwnerKey												int					not null,
		ChartererKey											int					not null,
		ProductQuantityKey										int					not null,
		CPDateKey												int					not null,
		InvoiceNumber											varchar(50)			null,		-- Degenerate Dimension Attributes
		InvoiceType												varchar(50)			null,
		InvoiceTo												varchar(500)		null,
		InvoiceStatus											varchar(100)		null,
		VesselFormula											varchar(100)		null,
		OfficeFormula											varchar(100)		null,
		BrokerFormula											varchar(100)		null,
		ChartererFormula										varchar(100)		null,
		OwnerFormula											varchar(100)		null,
		InvoiceGeneratedBy										varchar(100)		null,
		InvoiceAmount											decimal(18, 5)		null,		-- Metrics
		constraint [PK_Staging_Fact_Invoice_AltKey] primary key clustered 
		(
			InvoiceAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];