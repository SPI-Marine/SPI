drop table if exists Warehouse.Dim_Currency;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/30/2021
Description:	Creates the Warehouse.Dim_Currency table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create table Warehouse.Dim_Currency
	(
		CurrencyKey				int					not null identity(1, 1),
		CurrencyCode			varchar(5)			not null,
		CurrencyName			varchar(100)		not null,
		Type1HashValue			varbinary(16)		not null,
		RowCreatedDate			datetime			not null,
		RowUpdatedDate			datetime			not null,
		IsCurrentRow			char(1)				not null,
		constraint [PK_Warehouse_Dim_Currency_Key] primary key clustered 
		(
			CurrencyKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];