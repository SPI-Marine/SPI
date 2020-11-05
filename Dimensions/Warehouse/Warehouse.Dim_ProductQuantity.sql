/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/11/2020
Description:	Creates the Warehouse.Dim_ProductQuantity table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	11/05/2020	Modified quantity ranges
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_ProductQuantity;
go

create table Warehouse.Dim_ProductQuantity
	(
		ProductQuantityKey				int					not null identity(1, 1),
		ProductQuantityRange			varchar(50)			not null,
		MinimumQuantity					decimal(18, 4)		not null,
		MaximumQuantity					decimal(18, 4)		not null
		constraint [PK_Warehouse_Dim_ProductQuantity_Key] primary key clustered 
		(
			ProductQuantityKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];

go

-- Load quantity ranges
insert
		Warehouse.Dim_ProductQuantity
	values	('1-999', 1.0000, 1000.0000),
			('1001-1999', 1000.0000, 2000.0000),
			('2001-2999', 2000.0000, 3000.0000),
			('3001-3999', 3000.0000, 4000.0000),
			('4001-5999', 4000.0000, 6000.0000),
			('6001-7999', 6000.0000, 8000.0000),
			('8001-9999', 8000.0000, 10000.0000),
			('10001-14999', 10000.0000, 15000.0000),
			('15001-19999', 15000.0000, 20000.0000),
			('20001-24999', 20000.0000, 25000.0000),
			('25001-34999', 25000.0000, 35000.0000),
			('35000+', 35000.0000, 1000000.0000);


