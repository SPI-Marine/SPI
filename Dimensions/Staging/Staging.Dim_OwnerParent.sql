/*
==========================================================================================================
Author:			Brian Boswick
Create date:	09/15/2020
Description:	Creates the Staging.Dim_OwnerParent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_OwnerParent;
go

create table Staging.Dim_OwnerParent
	(
		OwnerParentAlternateKey		int					not null,
		OwnerParentName				varchar(500)		null,
		Notes						varchar(5000)		null,
		Type1HashValue				varbinary(16)		not null,
		RecordStatus				int					not null
		constraint [PK_Staging_Dim_OwnerParent_QBRecId] primary key clustered 
		(
			OwnerParentAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];