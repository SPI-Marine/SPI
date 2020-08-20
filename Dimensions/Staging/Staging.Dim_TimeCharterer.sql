/*
==========================================================================================================
Author:			Brian Boswick
Create date:	08/20/2020
Description:	Creates the Staging.Dim_TimeCharterer table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_TimeCharterer;
go

create table Staging.Dim_TimeCharterer
	(
		TimeChartererAlternateKey		int					not null,
		[Status]						varchar(500)		null,
		VaultTCFixtureNumber			varchar(500)		null,
		ContractType					varchar(500)		null,
		TCCPDate						datetime			null,
		CharterParty					varchar(500)		null,
		VesselFixedAsOfficial			varchar(500)		null,
		OwnerRef						varchar(500)		null,
		ChartererRef					varchar(500)		null,
		PeriodUnits						varchar(500)		null,
		OptionAdditionalPeriod1			numeric(20, 6)		null,
		OptionAdditionalPeriod1Units	varchar(500)		null,
		OptionAdditionalPeriod2			numeric(20, 6)		null,
		OptionAdditionalPeriod2Units	varchar(500)		null,
		ContractCommencement			date				null,
		ContractExpirey					date				null,
		RenewalDateDeclareBy			date				null,
		LaycanCommencement				datetime			null,
		LaycanCancelling				datetime			null,
		CurrencyForHire					varchar(500)		null,
		HirePaymentNotes				varchar(5000)		null,
		HireRateFirstPeriod				numeric(20, 6)		null,
		HireRateSecondPeriod			numeric(20, 6)		null,
		HireRateThirdPeriod				numeric(20, 6)		null,
		HirePayable						varchar(500)		null,
		FrequencyCommissionInvoiced		varchar(500)		null,
		AddressCommission				numeric(20, 6)		null,
		BrokerCommission				numeric(20, 6)		null,
		BrokerPIC						varchar(150)		null,
		OpsPIC							varchar(150)		null,
		SPIOffice						varchar(500)		null,
		Vessel							varchar(500)		null,
		OwnerFullStyle					varchar(500)		null,
		ChartererFullStyle				varchar(500)		null,
		Type1HashValue					varbinary(16)		not null,
		RecordStatus					int					not null
		constraint [PK_Staging_Dim_TimeCharterer_RecordId] primary key clustered 
		(
			TimeChartererAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];