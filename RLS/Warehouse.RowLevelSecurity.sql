drop view if exists Warehouse.RowLevelSecurity;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/09/2020
Description:	Creates the Warehouse.RowLevelSecurity table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Dan Pisarcik	06/17/2021	Changed table to view. Prototype for updated security table providing
				explicit combinations of permitted parents, fullstyles, and other fields associated with 
				post fixture
==========================================================================================================	
*/

set ansi_nulls on
go

set quoted_identifier on
go


create view [Warehouse].[RowLevelSecurity] as
with ProjectedSecurityTable as (
	select 
		postFix.PostFixtureKey
		,perm.UserId as PortalUserId
		,usr.Email
		,perm.Id as PermissionLevelId
		,lvl.Name as PermissionLevelName
		,postFix.ChartererParentRlsKey
		,postFix.ChartererRlsKey
		,postFix.OwnerParentRlsKey
		,postFix.OwnerRlsKey
		,postFix.ProductRlsKey
		,postFix.LoadRegionRlsKey
		,postFix.DischargeRegionRlsKey
		,perm.CpDataLimitYears
		,DATEADD(YEAR, -perm.CpDataLimitYears, GETDATE()) as MinCPDateToPull
		,usr.LogoUrl
	from dbo.SpiPortal_AppUserPostFixturePermissions perm (nolock)
		inner join dbo.SpiPortal_AspNetUsers usr (nolock) on usr.Id = perm.UserId
		inner join dbo.SpiPortal_PostFixturePermissionLevels lvl (nolock) on lvl.Id = perm.PermissionLevelId
		inner join Warehouse.Dim_PostFixture postFix (nolock)
			on 
				(
					(
						-- Filter Charterer Parent
						lvl.Name like 'Charterer%' and usr.ChartererParentRlsKey = postFix.ChartererParentRlsKey
						-- Filter Charterer(FS)
						and (
							lvl.Name = 'Charterer Parent'
							or (
								(lvl.Name = 'Charterer Full Style' or lvl.Name like 'Charterer - %')
								and postFix.ChartererRlsKey = perm.ChartererRlsKey
							)
						)
					)
					or (
						-- Filter Owner Parent
						lvl.Name like 'Owner%' and usr.OwnerParentRlsKey = postFix.OwnerParentRlsKey
						-- Filter Owner(FS)
						and (
							lvl.Name = 'Owner Parent'
							or (
								(lvl.Name = 'Owner Full Style' or lvl.Name like 'Owner - %')
								and postFix.OwnerRlsKey = perm.OwnerRlsKey
							)
						)
					)
				)
				-- Filter Product
				and (
					lvl.Name not like '%- Product'
					or lvl.Name like '%- Product' and postFix.ProductRlsKey = perm.ProductRlsKey
				)
				-- Filter Load Region
				and (
					lvl.Name not like '%- Load Region'
					or lvl.Name like '%- Load Region' and postFix.LoadRegionRlsKey = perm.LoadRegionRlsKey
				)
				-- Filter Discharge Region
				and (
					lvl.Name not like '%- Discharge Region'
					or lvl.Name like '%- Discharge Region' and postFix.DischargeRegionRlsKey = perm.DischargeRegionRlsKey
				)
)
select distinct * from ProjectedSecurityTable
GO


