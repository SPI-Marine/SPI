/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/21/2019
Description:	Creates the Staging.Dim_Parcel table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_Parcel;
go

create table Staging.Dim_Parcel
	(
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