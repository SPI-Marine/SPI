/*
==========================================================================================================
Author:			Brian Boswick
Create date:	01/08/2019
Description:	Creates the Staging.Fact_ParcelFinance table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_ParcelFinance;
go

create table Staging.Fact_ParcelFinance
	(
		PostFixtureAlternateKey	int					not null,
		ParcelAlternateKey		int					not null,
		PortKey					int					not null,
		BerthKey				int					not null,
		ProductKey				int					not null,
		PostFixtureKey			int					not null,
		VesselKey				int					not null,
		ChargeType				nvarchar(500)		null,		-- Degenerate Dimension Attributes
		ChargeDescription		nvarchar(500)		null,
		ParcelNumber			smallint			null,
		Charge					decimal(18, 4)		null,		-- Metrics
		RecordStatus			int					not null,
		constraint [PK_Staging_Fact_ParcelFinance_QBRecId] primary key clustered 
		(
			PostFixtureAlternateKey, ParcelAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];