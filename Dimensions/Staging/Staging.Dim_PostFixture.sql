/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/27/2018
Description:	Creates the Staging.Dim_PostFixture table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_PostFixture;
go

create table Staging.Dim_PostFixture
	(
		PostFixtureAlternateKey			int					not null,
		RelatedBroker					nvarchar(250)		null,
		RelatedOpsPrimary				nvarchar(250)		null,
		RelatedOpsBackup				nvarchar(250)		null,
		CPDate							date				null,
		CPForm							nvarchar(250)		null,
		DemurrageRate					decimal(18, 2)		null,
		TimeBar							decimal(18, 2)		null,
		AddressCommissionPercent		decimal(18, 2)		null,
		BrokerCommissionPercent			decimal(18, 2)		null,
		LaytimeAllowedLoad				decimal(18, 2)		null,
		LaytimeAllowedDisch				decimal(18, 2)		null,
		ShincReversible					nvarchar(10)			null,
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
		Type1HashValue					varbinary(16)		not null,
		RecordStatus					int					not null
		constraint [PK_Staging_Dim_PostFixture_QBRecId] primary key clustered 
		(
			PostFixtureAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];