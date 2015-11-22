using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql.Query;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Views {
	public static class QueryExtensions {
		public static bool ViewExists(this IQuery context, ObjectName viewName) {
			return context.ObjectExists(DbObjectType.View, viewName);
		}

		public static void DefineView(this IQuery context, ViewInfo viewInfo, bool replaceIfExists) {
			var tablesInPlan = viewInfo.QueryPlan.DiscoverTableNames();
			foreach (var tableName in tablesInPlan) {
				if (!context.UserCanSelectFromTable(tableName))
					throw new InvalidAccessException(context.UserName(), tableName);
			}

			if (context.ViewExists(viewInfo.ViewName)) {
				if (!replaceIfExists)
					throw new InvalidOperationException(
						String.Format("The view {0} already exists and the REPLCE clause was not specified.", viewInfo.ViewName));

				context.DropObject(DbObjectType.View, viewInfo.ViewName);
			}

			context.CreateObject(viewInfo);

			// The initial grants for a view is to give the user who created it
			// full access.
			using (var systemContext = context.Direct()) {
				systemContext.GrantToUserOnTable(viewInfo.ViewName, context.UserName(), Privileges.TableAll);
			}
		}

		public static void DefineView(this IQuery context, ObjectName viewName, IQueryPlanNode queryPlan, bool replaceIfExists) {
			// We have to execute the plan to get the TableInfo that represents the
			// result of the view execution.
			var table = queryPlan.Evaluate(context);
			var tableInfo = table.TableInfo.Alias(viewName);

			var viewInfo = new ViewInfo(tableInfo, null, queryPlan);
			context.DefineView(viewInfo, replaceIfExists);
		}

		public static void DropView(this IQuery context, ObjectName viewName) {
			DropView(context, viewName, false);
		}

		public static void DropView(this IQuery context, ObjectName viewName, bool ifExists) {
			context.DropViews(new[] { viewName }, ifExists);
		}

		public static void DropViews(this IQuery context, IEnumerable<ObjectName> viewNames) {
			DropViews(context, viewNames, false);
		}

		public static void DropViews(this IQuery context, IEnumerable<ObjectName> viewNames, bool onlyIfExists) {
			var viewNameList = viewNames.ToList();
			foreach (var tableName in viewNameList) {
				if (!context.UserCanDropObject(DbObjectType.View, tableName))
					throw new MissingPrivilegesException(context.UserName(), tableName, Privileges.Drop);
			}

			// If the 'only if exists' flag is false, we need to check tables to drop
			// exist first.
			if (!onlyIfExists) {
				// For each table to drop.
				foreach (var viewName in viewNameList) {
					// If view doesn't exist, throw an error
					if (!context.ViewExists(viewName)) {
						throw new ObjectNotFoundException(viewName, String.Format("The view '{0}' does not exist and cannot be dropped.", viewName));
					}
				}
			}

			foreach (var viewName in viewNameList) {
				// Does the table already exist?
				if (context.ViewExists(viewName)) {
					// Drop table in the transaction
					context.DropObject(DbObjectType.Table, viewName);

					// Revoke all the grants on the table
					context.RevokeAllGrantsOnView(viewName);
				}
			}
		}

		public static View GetView(this IQuery context, ObjectName viewName) {
			return context.GetObject(DbObjectType.View, viewName, AccessType.Read) as View;
		}

		public static IQueryPlanNode GetViewQueryPlan(this IQuery context, ObjectName viewName) {
			var view = context.GetView(viewName);
			return view == null ? null : view.QueryPlan;
		}
	}
}
