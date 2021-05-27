/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the Warehouse.Dim_Owner table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/13/2020	Source data from FullStyles table
Brian Boswick	12/16/2020	Added OwnerParentAlternateKey for RLS
Brian Boswick	05/27/2021	Removed GroupName
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_Owner;
go

create table Warehouse.Dim_Owner
	(
		OwnerKey					int					not null identity(1, 1),
		OwnerAlternateKey			int					not null,
		OwnerRlsKey					varchar(100)		null,
		OwnerParentAlternateKey		int					null,
		OwnerParentRlsKey			varchar(100)		null,
		FullStyleName				varchar(500)		null,
		OwnerParentName				varchar(500)		null,
		[Type]						varchar(500)		null,
		[Address]					varchar(500)		null,
		Type1HashValue				varbinary(16)		not null,
		RowCreatedDate				date				not null,
		RowUpdatedDate				date				not null,
		IsCurrentRow				char(1)				not null,
		constraint [PK_Warehouse_Dim_Owner_Key] primary key clustered 
		(
			OwnerKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];