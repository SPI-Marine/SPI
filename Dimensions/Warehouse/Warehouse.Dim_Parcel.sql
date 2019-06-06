/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/21/2019
Description:	Creates the Warehouse.Dim_Parcel table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	04/17/2019	Added ParcelNumber
Brian Boswick	06/04/2019	Added DeadfreightQty
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_Parcel;
go

create table Warehouse.Dim_Parcel
	(
		ParcelKey				int					not null identity(1, 1),
		ParcelAlternateKey		int					not null,
		BillLadingDate			date				null,
		ParcelFrtRate			decimal(18, 2)		null,
		OutTurnQty				decimal(18, 2)		null,
		ShipLoadedQty			decimal(18, 2)		null,
		ShipDischargeQty		decimal(18, 2)		null,
		NominatedQty			decimal(18, 2)		null,
		BLQty					decimal(18, 2)		null,
		Comments				nvarchar(2500)		null,
		Unit					nvarchar(20)		null,
		AgreedDemurrage			decimal(18, 2)		null,
		ClaimDemurrage			decimal(18, 2)		null,
		VaultDemurrage			decimal(18, 2)		null,
		IsAgreedProRated		varchar(15)			null,
		ParcelNumber			smallint			null,
		DeadfreightQty			decimal(18, 2)		null,
		Type1HashValue			varbinary(16)		not null,
		RowCreatedDate			datetime			not null,
		RowUpdatedDate			datetime			not null,
		IsCurrentRow			char(1)				not null,
		constraint [PK_Warehouse_Dim_Parcel_Key] primary key clustered 
		(
			ParcelKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];