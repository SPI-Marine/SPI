drop table if exists Warehouse.Dim_COA;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/28/2020
Description:	Creates the Warehouse.Dim_COA table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/10/2020	Added Charterer/Owner info
Brian Boswick	07/09/2021	Changed granularity to the Trade Lane level and added new fields
Brian Boswick	07/15/2021	Removed Trade Lane fields and split into separate Dimension
==========================================================================================================	
*/

create table Warehouse.Dim_COA
	(
		COAKey							int					not null identity(1, 1),
		COAAlternateKey					int					not null,
		COATitle						varchar(500)		null,
		AddressCommission				decimal(20, 6)		null,
		BrokerCommission				decimal(20, 6)		null,
		[Status]						varchar(50)			null,
		[P&C]							varchar(50)			null,
		COADate							date				null,
		AddendumDate					date				null,
		AddendumExpiryDate				date				null,
		AddendumCommencementDate		date				null,
		RenewalDeclareByDate			date				null,
		ContractCommencementDate		date				null,
		ContractCancellingDate			date				null,
		ChartererParent					varchar(150)		null,
		OwnerParent						varchar(150)		null,
		ChartererFullStyle				varchar(150)		null,
		OwnerFullStyle					varchar(150)		null,
		BrokerRegion					varchar(150)		null,
		StatusFormula					varchar(500)		null,
		RenewalStatus					varchar(500)		null,
		ContractNameType				varchar(500)		null,
		COAorServiceAgreement			varchar(500)		null,
		[Broker]						varchar(500)		null,
		OpsPICFullNameTM				varchar(500)		null,
		SPIOffice						varchar(500)		null,
		CPForm							varchar(500)		null,
		FreightBasisEntry				varchar(500)		null,
		[Period]						varchar(500)		null,
		Mos_Years						varchar(500)		null,
		[Option]						varchar(500)		null,
		TermsforOptionalPeriodAsia		varchar(500)		null,
		DemurrageTimeBar				varchar(500)		null,
		DemOutcome1						varchar(500)		null,
		DemurrageCOANotes				varchar(5000)		null,
		Type1HashValue					varbinary(16)		not null,
		RowCreatedDate					date				not null,
		RowUpdatedDate					date				not null,
		IsCurrentRow					char(1)				not null,
		constraint [PK_Warehouse_Dim_COA_Key] primary key clustered 
		(
			COAKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];