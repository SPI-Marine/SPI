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
Brian Boswick	12/17/2020	Changed ETL for new RLS fields
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
																PortalUserId,
																PortalUserEmail,
																PermissionLevelId,
																PermissionLevelName,
																ChartererParentRlsKey,
																ChartererRlsKey,
																OwnerParentRlsKey,
																OwnerRlsKey,
																ProductRlsKey,
																LoadRegionRlsKey,
																DischargeRegionRlsKey,
																MinCPDateToPull
															)
		select
			u.Id PortalUserId
			,u.Email PortalUserEmail
			,p.PermissionLevelId PermissionLevelId
			,pl.[Name] PermissionLevelName
			,u.ChartererParentRlsKey
			,p.ChartererRlsKey
			,u.OwnerParentRlsKey
			,p.OwnerRlsKey
			,p.ProductRlsKey
			,p.LoadRegionRlsKey
			,p.DischargeRegionRlsKey
			,convert(date, dateadd(year, -p.CpDataLimitYears, getdate())) MinCPDateToPull
		from
			SpiPortal_AspNetUsers u
				left outer join SpiPortal_AppUserPostFixturePermissions p 
					on p.UserId = u.Id
				left outer join SpiPortal_PostFixturePermissionLevels pl
					on pl.Id = p.PermissionLevelId
		where
			p.PermissionLevelId is not null;
	end try
	begin catch
		select @ErrorMsg = 'Loading RowLevelSecurity records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end