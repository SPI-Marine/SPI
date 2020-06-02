/*
==========================================================================================================
Author:			Brian Boswick
Create date:	06/01/2020
Description:	Used to store the SOF Event durations
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.SOFEvent_Durations;
go

create table Staging.SOFEvent_Durations
	(
		EventAlternateKey			int					not null,
		PostFixtureAlternateKey		int					not null,
		ParcelBerthAlternateKey		int					not null,
		EventTypeId					int					null,
		EventType					nvarchar(250)		null,
		Duration					decimal(18, 5)		null,
		EventStartDateTime			datetime			null,
		NextEventStartDateTime		datetime			null,
		LoadDischarge				varchar(50)			null,
		IsLaytime					char(1)				null,
		IsPumpingTime				char(1)				null,
		LaytimeUsedProrated			decimal(20, 6)		null
		constraint [PK_Staging_SOFEvent_Duration_QBRecId] primary key clustered 
		(
			EventAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];