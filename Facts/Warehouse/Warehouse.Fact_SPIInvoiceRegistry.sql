drop table if exists Warehouse.Fact_SPIInvoiceRegistry;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/24/2020
Description:	Creates the Warehouse.Fact_SPIInvoiceRegistry table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	08/27/2020	Added InvoiceTypeCategory
Brian Boswick	10/01/2020	Added RegionFormula
Brian Boswick	04/29/2021	Modified Invoice Type fields. Added InvoiceTypeSubCategory
Brian Boswick	07/12/2021	Removed COAKey
==========================================================================================================	
*/

create table Warehouse.Fact_SPIInvoiceRegistry
	(
		InvoiceKey												int					not null identity(1, 1),
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
		TimeChartererKey										int					not null,
		InvoiceNumber											varchar(50)			null,		-- Degenerate Dimension Attributes
		InvoiceTypeDetailed										varchar(50)			null,
		InvoiceTo												varchar(500)		null,
		InvoiceStatus											varchar(100)		null,
		VesselFormula											varchar(100)		null,
		OfficeFormula											varchar(100)		null,
		RegionFormula											varchar(100)		null,
		BrokerFormula											varchar(100)		null,
		ChartererFormula										varchar(100)		null,
		OwnerFormula											varchar(100)		null,
		InvoiceGeneratedBy										varchar(100)		null,
		CreditAppliedAgainstInvoiceNumber						varchar(100)		null,
		CurrencyInvoice											varchar(100)		null,
		InvoiceTypeCategory										varchar(100)		null,
		InvoiceTypeSubCategory									varchar(100)		null,
		InvoiceAmount											decimal(18, 5)		null,		-- Metrics
		RowCreatedDate											date				not null,
		constraint [PK_Warehouse_Fact_SPIInvoiceRegistry_Key] primary key clustered 
		(
			InvoiceKey, LoadPortKey, DischargePortKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];