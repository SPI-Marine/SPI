drop table if exists Staging.Dim_COA;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/28/2020
Description:	Creates the Staging.Dim_COA table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/10/2020	Added Charterer/Owner info
Brian Boswick	07/09/2021	Changed granularity to the Trade Lane level and added new fields
==========================================================================================================	
*/

create table Staging.Dim_COA
	(
		TradeLaneAlternateKey			int					not null,
		COAAlternateKey					int					null,
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
		TradelaneNumLiftingsMinEntry	int					null,
		TradelaneNumLiftingsMaxEntry	int					null,
		TradeLaneLiftingQtyMinEntry		int					null,
		TradeLaneLiftingQtyMaxEntry		int					null,
		LoadOption						varchar(500)		null,
		FreightDetails					varchar(5000)		null,
		LiftingRequirementOptions		varchar(5000)		null,
		Type1HashValue					varbinary(16)		not null,
		RecordStatus					int					not null
		constraint [PK_Staging_Dim_COA_QBRecId] primary key clustered 
		(
			TradeLaneAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];