/*
==========================================================================================================
Author:			Brian Boswick
Create date:	09/15/2020
Description:	Creates the Warehouse.Dim_ChartererParent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_ChartererParent;
go

create table Warehouse.Dim_ChartererParent
	(
		ChartererParentKey				int					not null identity(1, 1),
		ChartererParentAlternateKey		int					not null,
		ChartererParentName				varchar(500)		null,
		Notes							varchar(5000)		null,
		[Type]							varchar(500)		null,
		Type1HashValue					varbinary(16)		not null,
		RowCreatedDate					date				not null,
		RowUpdatedDate					date				not null,
		IsCurrentRow					char(1)				not null,
		constraint [PK_Warehouse_Dim_ChartererParent_Key] primary key clustered 
		(
			ChartererParentKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];