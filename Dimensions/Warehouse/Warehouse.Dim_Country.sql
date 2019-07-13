/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/12/2019
Description:	Creates the Warehouse.Dim_Country table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_Country;
go

create table Warehouse.Dim_Country
	(
		CountryKey				int					not null identity(1, 1),
		CountryAlternateKey		int					not null,
		CountryName				nvarchar(250)		not null,
		Type1HashValue			varbinary(16)		not null,
		RowCreatedDate			date				not null,
		RowUpdatedDate			date				not null,
		IsCurrentRow			char(1)				not null,
		constraint [PK_Warehouse_Dim_Country_Key] primary key clustered 
		(
			CountryKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];