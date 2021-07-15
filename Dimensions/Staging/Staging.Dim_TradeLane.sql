drop table if exists Staging.Dim_TradeLane;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/15/2021
Description:	Creates the Staging.Dim_TradeLane table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create table Staging.Dim_TradeLane
	(
		TradeLaneAlternateKey			int					not null,
		COAKey							int					not null,
		TradelaneNumLiftingsMinEntry	int					null,
		TradelaneNumLiftingsMaxEntry	int					null,
		TradeLaneLiftingQtyMinEntry		int					null,
		TradeLaneLiftingQtyMaxEntry		int					null,
		LoadOption						varchar(500)		null,
		FreightDetails					varchar(5000)		null,
		LiftingRequirementOptions		varchar(5000)		null,
		TradeLaneTitle					varchar(500)		null,
		Type1HashValue					varbinary(16)		not null,
		RecordStatus					int					not null
		constraint [PK_Staging_Dim_TradeLane_QBRecId] primary key clustered 
		(
			TradeLaneAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];