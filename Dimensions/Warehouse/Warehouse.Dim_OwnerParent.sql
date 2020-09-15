/*
==========================================================================================================
Author:			Brian Boswick
Create date:	09/15/2020
Description:	Creates the Warehouse.Dim_OwnerParent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_OwnerParent;
go

create table Warehouse.Dim_OwnerParent
	(
		OwnerParentKey				int					not null identity(1, 1),
		OwnerParentAlternateKey		int					not null,
		OwnerParentName				varchar(500)		null,
		Notes						varchar(5000)		null,
		Type1HashValue				varbinary(16)		not null,
		RowCreatedDate				date				not null,
		RowUpdatedDate				date				not null,
		IsCurrentRow				char(1)				not null,
		constraint [PK_Warehouse_Dim_OwnerParent_Key] primary key clustered 
		(
			OwnerParentKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];