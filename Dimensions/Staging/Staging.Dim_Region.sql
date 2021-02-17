/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/09/2020
Description:	Creates the Staging.Dim_Region table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	01/07/2021	Added RegionRlsKey
==========================================================================================================	
*/

drop table if exists Staging.Dim_Region;
go

create table Staging.Dim_Region
	(
		RegionAlternateKey		int					not null,
		RegionRlsKey			varchar(15)			null,
		RegionName				varchar(250)		null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null,
		constraint [PK_Staging_Dim_Region_QBRecId] primary key clustered 
		(
			RegionAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];