/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the Warehouse.Dim_Charterer table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/13/2020	Source data from FullStyles table
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_Charterer;
go

create table Warehouse.Dim_Charterer
	(
		ChartererKey				int					not null identity(1, 1),
		ChartererAlternateKey		int					not null,
		FullStyleName				varchar(500)		null,
		ChartererParentName			varchar(500)		null,
		[Type]						varchar(500)		null,
		[Address]					varchar(500)		null,
		GroupName					varchar(500)		null,
		Type1HashValue				varbinary(16)		not null,
		RowCreatedDate				date				not null,
		RowUpdatedDate				date				not null,
		IsCurrentRow				char(1)				not null,
		constraint [PK_Warehouse_Dim_Charterer_Key] primary key clustered 
		(
			ChartererKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];