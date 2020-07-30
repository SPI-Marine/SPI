/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/28/2018
Description:	Creates the Warehouse.Dim_PostFixture table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	03/19/2019	Added Broker name, Charterer and Owner
Brian Boswick	04/20/2019	Added COA related information
Brian Boswick	04/25/2019	Added LaycanCancellingOriginal, LaycanCancellingFinal_QBC,
							LaycanCommencementFinal_QBC
Brian Boswick	06/13/2019	Added Region
Brian Boswick	07/01/2019	Added four new fields from QB
Brian Boswick	07/17/2019	Added OwnerParent and ChartererParent fields
Brian Boswick	09/23/2019	Added FixtureType
Brian Boswick	01/07/2020	Added GroupName
Brian Boswick	02/19/2020	Added COA_Title_Admin
Brian Boswick	05/21/2020	Added Load and Discharge Port Region
Brian Boswick	05/22/2020	Added Product ETL logic
Brian Boswick	07/13/2020	Added LaycanStatus
Brian Boswick	07/20/2020	Added HoseOffDateFinal
Brian Boswick	07/22/2020	Added FrtRateProjection
Brian Boswick	07/28/2020	Added VoyageSummaryReportComments
Brian Boswick	07/09/2020	Removed COA fields
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_PostFixture;
go

create table Warehouse.Dim_PostFixture
	(
		PostFixtureKey					int					not null identity(1, 1),
		PostFixtureAlternateKey			int					not null,
		BrokerEmail						nvarchar(250)		null,
		BrokerFirstName					nvarchar(250)		null,
		BrokerLastName					nvarchar(250)		null,
		BrokerFullName					nvarchar(250)		null,
		OwnerFullStyle					nvarchar(250)		null,
		ChartererFullStyle				nvarchar(250)		null,
		OwnerParent						nvarchar(250)		null,
		ChartererParent					nvarchar(250)		null,
		RelatedOpsPrimary				nvarchar(250)		null,
		RelatedOpsBackup				nvarchar(250)		null,
		CPDate							date				null,
		CPForm							nvarchar(250)		null,
		DemurrageRate					decimal(18, 2)		null,
		TimeBar							decimal(18, 2)		null,
		HoseOffDateFinal				date				null,
		AddressCommissionPercent		decimal(18, 2)		null,
		BrokerCommissionPercent			decimal(18, 2)		null,
		LaytimeAllowedLoad				varchar(100)		null,
		LaytimeAllowedDisch				varchar(100)		null,
		ShincReversible					nvarchar(10)		null,
		VesselNameSnap					nvarchar(100)		null,
		DemurrageAmountAgreed			decimal(18, 2)		null,
		CharterInvoiced					char(1)				null,
		PaymentType						nvarchar(100)		null,
		FreightLumpSumEntry				decimal(18, 2)		null,
		DischargeFAC					char(1)				null,
		LaytimeOption					nvarchar(100)		null,
		OwnersReference					nvarchar(100)		null,
		CharterersReference				nvarchar(100)		null,
		CurrencyInvoice					nvarchar(100)		null,
		CharteringPicSnap				nvarchar(100)		null,
		OperationsPicSnap				nvarchar(100)		null,
		BrokerCommDemurrage				char(1)				null,
		AddCommDeadFreight				char(1)				null,
		DemurrageClaimReceived			date				null,
		VoyageNumber					nvarchar(100)		null,
		LaycanToBeAmended				char(1)				null,
		LaycanCancellingAmended			date				null,
		LaycanCommencementAmended		date				null,
		CurrencyCP						nvarchar(100)		null,
		FixtureStatus					nvarchar(200)		null,
		LaytimeAllowedTotalLoad			decimal(18, 2)		null,
		LaytimeAllowedTotalDisch		decimal(18, 2)		null,
		FrtRatePmt						decimal(18, 2)		null,
		BrokerFrtComm					decimal(18, 2)		null,
		P2FixtureRefNum					nvarchar(100)		null,
		VesselFixedOfficial				nvarchar(100)		null,
		LaycanCommencementOriginal		date				null,
		LaycanCancellingOriginal		date				null,
		LaycanCancellingFinal_QBC		date				null,
		LaycanCommencementFinal_QBC		date				null,
		SPIFixtureStatus				varchar(100)		null,
		Region							varchar(100)		null,
		LAF_Disch_Mtph_QBC				decimal(18, 2)		null,
		LAF_Load_Mtph_QBC				decimal(18, 2)		null,
		LAF_Total_hrs_QBC				decimal(18, 2)		null,
		LaytimeAllowedTypeFixture_QBC	varchar(100)		null,
		FixtureType						varchar(500)		null,
		GroupName						varchar(500)		null,
		SPIOffice						varchar(500)		null,
		LoadRegion						varchar(500)		null,
		DischargeRegion					varchar(500)		null,
		Product							varchar(500)		null,
		LaycanStatus					varchar(100)		null,
		FrtRateProjection				decimal(18, 6)		null,
		VoyageSummaryReportComments		varchar(1000)		null,
		Type1HashValue					varbinary(16)		not null,
		RowCreatedDate					date				not null,
		RowUpdatedDate					date				not null,
		IsCurrentRow					char(1)				not null,
		constraint [PK_Warehouse_Dim_PostFixture_Key] primary key clustered 
		(
			PostFixtureKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];