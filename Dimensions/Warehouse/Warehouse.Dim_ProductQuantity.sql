/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/11/2020
Description:	Creates the Warehouse.Dim_ProductQuantity table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
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
	values	('1-1000', 1.0000, 1000.0000),
			('1001-2000', 1000.0001, 2000.0000),
			('2001-3000', 2000.0001, 3000.0000),
			('3001-4000', 3000.0001, 4000.0000),
			('4001-6000', 4000.0001, 6000.0000),
			('6001-8000', 6000.0001, 8000.0000),
			('8001-10000', 8000.0001, 10000.0000),
			('10001-15000', 10000.0001, 15000.0000),
			('15001-20000', 15000.0001, 20000.0000),
			('20001-25000', 20000.0001, 25000.0000),
			('25001-35000', 25000.0001, 35000.0000),
			('35001+', 35000.0001, 1000000.0000);


