/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/21/2019
Description:	Creates the Staging.Dim_Parcel table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	04/17/2019	Added ParcelNumber
Brian Boswick	06/04/2019	Added DeadfreightQty
==========================================================================================================	
*/

drop table if exists Staging.Dim_Parcel;
go

create table Staging.Dim_Parcel
	(
		ParcelAlternateKey		int					not null,
		BillLadingDate			date				null,
		ParcelFrtRate			decimal(18, 5)		null,
		OutTurnQty				decimal(18, 5)		null,
		ShipLoadedQty			decimal(18, 5)		null,
		ShipDischargeQty		decimal(18, 5)		null,
		NominatedQty			decimal(18, 5)		null,
		BLQty					decimal(18, 5)		null,
		Comments				nvarchar(2500)		null,
		Unit					nvarchar(20)		null,
		AgreedDemurrage			decimal(18, 5)		null,
		ClaimDemurrage			decimal(18, 5)		null,
		VaultDemurrage			decimal(18, 5)		null,
		IsAgreedProRated		varchar(15)			null,
		ParcelNumber			smallint			null,
		DeadfreightQty			decimal(18, 5)		null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null,
		constraint [PK_Staging_Dim_Parcel_QBRecId] primary key clustered 
		(
			ParcelAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];