/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the Staging.Dim_Owner table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/13/2020	Source data from FullStyles table
Brian Boswick	12/16/2020	Added OwnerParentAlternateKey for RLS
==========================================================================================================	
*/

drop table if exists Staging.Dim_Owner;
go

create table Staging.Dim_Owner
	(
		OwnerAlternateKey		int					not null,
		OwnerRlsKey				varchar(100)		null,
		OwnerParentAlternateKey	int					null,
		OwnerParentRlsKey		varchar(100)		null,
		FullStyleName			varchar(500)		null,
		OwnerParentName			varchar(500)		null,
		[Type]					varchar(500)		null,
		[Address]				varchar(500)		null,
		GroupName				varchar(500)		null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null
		constraint [PK_Staging_Dim_Owner_QBRecId] primary key clustered 
		(
			OwnerAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];