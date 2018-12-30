/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/18/2018
Description:	Creates the Staging.Dim_Berth table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_Berth;
go

create table Staging.Dim_Berth
	(
		BerthAlternateKey		int					not null,
		BerthName				nvarchar(250)		not null,
		DraftRestriction		decimal(18, 4)		null,
		LOARestriction			decimal(18, 4)		null,
		ProductRestriction		nvarchar(2500)		null,
		ExNames					nvarchar(100)		null,
		UniqueId				nvarchar(100)		null,
		UpRiverPorts			nvarchar(100)		null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null,
		constraint [PK_Staging_Dim_Berth_QBRecId] primary key clustered 
		(
			BerthAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];