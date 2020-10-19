set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadRowLevelSecurity;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	10/19/2020
Description:	Creates the LoadRowLevelSecurity stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadRowLevelSecurity
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Warehouse table
	if object_id(N'Warehouse.RowLevelSecurity', 'U') is not null
		truncate table Warehouse.RowLevelSecurity;

	begin try
		insert
				Warehouse.RowLevelSecurity with (tablock)	(
																RecordID,
																Product,
																ChartererParent,
																OwnerParent,
																UserName,
																LoadRegion,
																DischargeRegion,
																FullStyleName,
																[GUID],
																MinCPDateToPull
															)
		select
				rls.RecordID						RecordID,
				rls.Product							Product,
				rls.ChartererParent					ChartererParent,
				rls.OwnerParent						OwnerParent,
				rls.UserName						UserName,
				rls.LoadRegion						LoadRegion,
				rls.DischargeRegion					DischargeRegion,
				rls.FullStyleName					FullStyleName,
				rls.[GUID]							[GUID],
				rls.MinCPDateToPull					MinCPDateToPull
			from
				Staging.RowLevelSecurity rls;
	end try
	begin catch
		select @ErrorMsg = 'Staging RowLevelSecurity records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end