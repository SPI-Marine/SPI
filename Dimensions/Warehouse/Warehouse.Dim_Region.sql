/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/09/2020
Description:	Creates the Warehouse.Dim_Region table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_Region;
go

create table Warehouse.Dim_Region
	(
		RegionKey				int					not null identity(1, 1),
		RegionAlternateKey		int					not null,
		RegionName				nvarchar(250)		not null,
		Type1HashValue			varbinary(16)		not null,
		RowCreatedDate			date				not null,
		RowUpdatedDate			date				not null,
		IsCurrentRow			char(1)				not null,
		constraint [PK_Warehouse_Dim_Region_QBRecId] primary key clustered 
		(
			RegionAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];