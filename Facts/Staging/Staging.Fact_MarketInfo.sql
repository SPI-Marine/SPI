drop table if exists Staging.Fact_MarketInfo;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the Staging.Fact_MarketInfo table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/20/2020	Added BasisDataEntry
Brian Boswick	10/16/2020	Added PandC
Brian Boswick	11/01/2020	Added LastModifiedBy
==========================================================================================================	
*/

create table Staging.Fact_MarketInfo
	(
		MarketInfoAlternateKey					int					not null,
		ProductKey								int					not null,
		LoadPortKey								int					not null,
		DischargePortKey						int					not null,
		ReportDateKey							int					not null,
		CommencementDateKey						int					not null,
		CancellingDateKey						int					not null,
		VesselKey								int					not null,
		OwnerParentKey							int					not null,
		ChartererParentKey						int					not null,
		ProductQuantityKey						int					not null,
		LoadPort2								varchar(500)		null,		-- Degenerate Dimension Attributes
		DischargePort2							varchar(500)		null,
		DischargePort3							varchar(500)		null,
		MarketInfoType							varchar(500)		null,
		Unit									varchar(50)			null,
		BasisDataEntry							varchar(500)		null,
		PandC									varchar(50)			null,
		LastModifiedBy							varchar(250)		null,
		FreightRatePayment						numeric(18, 5)		null,		-- Metrics
		ProductQuantity							numeric(18, 5)		null,
		constraint [PK_Staging_Fact_MarketInfo_AltKey] primary key clustered 
		(
			MarketInfoAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];