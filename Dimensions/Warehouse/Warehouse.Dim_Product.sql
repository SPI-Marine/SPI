drop table if exists Warehouse.Dim_Product;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Warehouse.Dim_Product table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	04/10/2019	Added ProductType
==========================================================================================================	
*/

create table Warehouse.Dim_Product
	(
		ProductKey				int					not null identity(1, 1),
		ProductAlternateKey		int					not null,
		ProductRlsKey			varchar(100)		not null,
		ProductName				nvarchar(250)		not null,
		SpecificGravity			decimal(18, 4)		null,
		RequiredCoating			nvarchar(250)		null,
		EU						nvarchar(250)		null,
		CIQ						nvarchar(100)		null,
		NIOP					nvarchar(100)		null,
		Notes					nvarchar(1000)		null,
		LiquidType				nvarchar(100)		null,
		ProductType				nvarchar(100)		null,
		Type1HashValue			varbinary(16)		not null,
		RowCreatedDate			datetime			not null,
		RowUpdatedDate			datetime			not null,
		IsCurrentRow			char(1)				not null,
		constraint [PK_Warehouse_Dim_Product_Key] primary key clustered 
		(
			ProductKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];