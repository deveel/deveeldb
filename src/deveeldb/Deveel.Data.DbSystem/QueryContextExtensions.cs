// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	public static class QueryContextExtensions {
		public static User User(this IQueryContext context) {
			return context.Session.SessionInfo.User;
		}

		public static string UserName(this IQueryContext context) {
			return context.User().Name;
		}

		internal static IQueryContext ForSystemUser(this IQueryContext queryContext) {
			return new SystemQueryContext(queryContext.Session.Transaction, queryContext.CurrentSchema);
		}

		public static IDatabase Database(this IQueryContext context) {
			return context.Session.Database;
		}

		public static bool IgnoreIdentifiersCase(this IQueryContext context) {
			return context.Session.IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this IQueryContext context, bool value) {
			context.Session.IgnoreIdentifiersCase(value);
		}

		public static void AutoCommit(this IQueryContext context, bool value) {
			context.Session.AutoCommit(value);
		}

		public static bool AutoCommit(this IQueryContext context) {
			return context.Session.AutoCommit();
		}

		public static string CurrentSchema(this IQueryContext context) {
			return context.Session.CurrentSchema;
		}

		public static void CurrentSchema(this IQueryContext context, string value) {
			context.Session.CurrentSchema(value);
		}

		public static void ParameterStyle(this IQueryContext context, QueryParameterStyle value) {
			context.Session.ParameterStyle(value);
		}

		public static QueryParameterStyle ParameterStyle(this IQueryContext context) {
			return context.Session.ParameterStyle();
		}

		public static IDatabaseContext DatabaseContext(this IQueryContext context) {
			return context.Session.Database.DatabaseContext;
		}

		public static ISystemContext SystemContext(this IQueryContext context) {
			return context.DatabaseContext().SystemContext;
		}

		#region Objects

		public static bool ObjectExists(this IQueryContext context, ObjectName objectName) {
			return context.Session.ObjectExists(objectName);
		}

		public static bool ObjectExists(this IQueryContext context, DbObjectType objectType, ObjectName objectName) {
			return context.Session.ObjectExists(objectType, objectName);
		}

		public static IDbObject GetObject(this IQueryContext context, DbObjectType objType, ObjectName objName) {
			// TODO: throw a specialized exception
			if (!context.UserCanAccessObject(objType, objName))
				throw new InvalidOperationException();

			return context.Session.GetObject(objType, objName);
		}

		public static void CreateObject(this IQueryContext context, IObjectInfo objectInfo) {
			// TODO: throw a specialized exception
			if (!context.UserCanCreateObject(objectInfo.ObjectType, objectInfo.FullName))
				throw new InvalidOperationException();

			context.Session.CreateObject(objectInfo);
		}

		public static ObjectName ResolveObjectName(this IQueryContext context, string name) {
			return context.Session.ResolveObjectName(name);
		}

		public static ObjectName ResolveObjectName(this IQueryContext context, DbObjectType objectType, ObjectName objectName) {
			return context.Session.ResolveObjectName(objectType, objectName);
		}

		#endregion

		#region Schemata

		public static void CreateSchema(this IQueryContext context, string name, string type) {
			if (!context.UserCanCreateSchema())
				throw new InvalidOperationException();		// TODO: throw a specialized exception

			context.CreateObject(new SchemaInfo(name, type));
		}

		public static bool SchemaExists(this IQueryContext context, string name) {
			return context.ObjectExists(DbObjectType.Schema, new ObjectName(name));
		}

		public static ObjectName ResolveSchemaName(this IQueryContext context, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			return context.ResolveObjectName(DbObjectType.Schema, new ObjectName(name));
		}

		#endregion

		#region Tables

		public static ObjectName ResolveTableName(this IQueryContext context, ObjectName tableName) {
			return context.Session.ResolveTableName(tableName);
		}

		public static ObjectName ResolveTableName(this IQueryContext context, string name) {
			var schema = context.CurrentSchema;
			if (String.IsNullOrEmpty(schema))
				throw new InvalidOperationException("Default schema not specified in the context.");
				
			var objSchemaName = context.ResolveSchemaName(schema);
			if (objSchemaName == null)
				throw new InvalidOperationException(String.Format("The default schema of the session '{0}' is not defined in the database.", schema));

			return context.ResolveTableName(new ObjectName(objSchemaName, name));
		}

		public static bool TableExists(this IQueryContext context, ObjectName tableName) {
			return context.ObjectExists(DbObjectType.Table, tableName);
		}

		public static void CreateTable(this IQueryContext context, TableInfo tableInfo) {
			CreateTable(context, tableInfo, false);
		}

		public static void CreateTable(this IQueryContext context, TableInfo tableInfo, bool onlyIfNotExists) {
			CreateTable(context, tableInfo, onlyIfNotExists, false);
		}

		public static void CreateTable(this IQueryContext context, TableInfo tableInfo, bool onlyIfNotExists, bool temporary) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");

			var tableName = tableInfo.TableName;

			if (!context.UserCanCreateTable(tableName))
				throw new InvalidOperationException(String.Format("The user '{0}' is not allowed to create table '{1}'.",
					context.User().Name, tableName));

			if (context.TableExists(tableName)) {
				if (!onlyIfNotExists)
					throw new InvalidOperationException(
						String.Format("The table {0} already exists and the IF NOT EXISTS clause was not specified.", tableName));

				return;
			}

			context.Session.CreateTable(tableInfo, temporary);			
		}

		public static ITable GetTable(this IQueryContext context, ObjectName tableName) {
			var table = context.GetCachedTable(tableName.FullName) as ITable;
			if (table == null) {
				table = context.Session.GetTable(tableName);
				if (table != null) {
					table = new UserContextTable(context, table);
					context.CacheTable(tableName.FullName, table);
				}
			}

			return table;
		}

		public static IMutableTable GetMutableTable(this IQueryContext context, ObjectName tableName) {
			return context.GetTable(tableName) as IMutableTable;
		}

		public static ITable GetCachedTable(this IQueryContext context, string cacheKey) {
			if (context.TableCache == null)
				return null;

			return context.TableCache.Get(cacheKey) as ITable;
		}

		public static void CacheTable(this IQueryContext context, string cacheKey, ITable table) {
			if (context.TableCache == null)
				return;

			context.TableCache.Set(cacheKey, table);
		}

		public static void ClearCachedTables(this IQueryContext context) {
			if (context.TableCache == null)
				return;

			context.TableCache.Clear();
		}

		public static ITableQueryInfo GetTableQueryInfo(this IQueryContext context, ObjectName tableName, ObjectName alias) {
			return context.Session.GetTableQueryInfo(tableName, alias);
		}

		#region Operations

		public static int DeleteFrom(this IQueryContext context, ObjectName tableName, SqlQueryExpression query) {
			return DeleteFrom(context, tableName, query, -1);
		}

		public static int DeleteFrom(this IQueryContext context, ObjectName tableName, SqlExpression expression) {
			return DeleteFrom(context, tableName, expression, -1);
		}

		public static int DeleteFrom(this IQueryContext context, ObjectName tableName, SqlExpression expression, int limit) {
			if (expression is SqlQueryExpression)
				return context.DeleteFrom(tableName, (SqlQueryExpression) expression, limit);

			var table = context.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);

			var queryExpression = new SqlQueryExpression(new List<SelectColumn> {SelectColumn.Glob("*")});
			queryExpression.FromClause.AddTable(tableName.Name);
			queryExpression.WhereExpression = expression;

			var planExpression = queryExpression.Evaluate(context, null);
			var plan = (SqlQueryObject) ((SqlConstantExpression) planExpression).Value.Value;
			var deleteSet = plan.QueryPlan.Evaluate(context);

			return context.DeleteFrom(tableName, deleteSet, limit);
		}

		public static int DeleteFrom(this IQueryContext context, ObjectName tableName, SqlQueryExpression query, int limit) {
			IQueryPlanNode plan;

			try {
				var planValue = query.EvaluateToConstant(context, null);
				if (planValue == null)
					throw new InvalidOperationException();

				if (!(planValue.Type is QueryType))
					throw new InvalidOperationException();

				plan = ((SqlQueryObject) planValue.Value).QueryPlan;
			} catch (QueryException) {
				throw;
			} catch (SecurityException) {
				throw;
			} catch (Exception ex) {
				throw new InvalidOperationException(String.Format("Could not delete from table '{0}': unable to form the delete set.", tableName), ex);
			}

			var deleteSet = plan.Evaluate(context);
			return context.DeleteFrom(tableName, deleteSet, limit);
		}

		public static int DeleteFrom(this IQueryContext context, ObjectName tableName, ITable deleteSet, int limit) {
			if (!context.UserCanDeleteFromTable(tableName))
				throw new InvalidOperationException();

			var table = context.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);

			return table.Delete(deleteSet, limit);
		}

		public static int UpdateTable(this IQueryContext context, ObjectName tableName, IQueryPlanNode queryPlan,
			IEnumerable<SqlAssignExpression> assignments, int limit) {
			var columnNames = assignments.Select(x => x.Reference).Cast<SqlReferenceExpression>().Select(x => x.ReferenceName.Name).ToArray();
			if (!context.UserCanUpdateTable(tableName, columnNames))
				throw new InvalidOperationException();

			if (!context.UserCanSelectFromPlan(queryPlan))
				throw new InvalidOperationException();

			var table = context.GetMutableTable(tableName);
			if (table == null)
				throw new InvalidOperationException();

			var updateSet = queryPlan.Evaluate(context);
			return table.Update(context, updateSet, assignments, limit);
		}

		public static void InsertIntoTable(this IQueryContext context, ObjectName tableName, IEnumerable<SqlAssignExpression> assignments) {
			var columnNames =
				assignments.Select(x => x.Reference).Cast<SqlReferenceExpression>().Select(x => x.ReferenceName.Name).ToArray();
			if (!context.UserCanInsertIntoTable(tableName, columnNames))
				throw new InvalidOperationException();

			var table = context.GetMutableTable(tableName);

			var row = table.NewRow();
			foreach (var expression in assignments) {
				row.EvaluateAssignment(expression, context);
			}

			table.AddRow(row);
		}

		public static int InsertIntoTable(this IQueryContext context, ObjectName tableName,
			IEnumerable<SqlAssignExpression[]> assignments) {
			int insertCount = 0;

			foreach (var assignment in assignments) {
				context.InsertIntoTable(tableName, assignment);
				insertCount++;
			}

			return insertCount;
		}

		#endregion

		#region Constraints

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string[] columnNames) {
			AddPrimaryKey(context, tableName, columnNames, null);
		}

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string[] columnNames, string constraintName) {
			if (!context.UserCanAlterTable(tableName))
				throw new InvalidOperationException();	// TODO: throw a specialized exception

			context.Session.AddPrimaryKey(tableName, columnNames, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string columnName) {
			AddPrimaryKey(context, tableName, columnName, null);
		}

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string columnName, string constraintName) {
			context.AddPrimaryKey(tableName, new []{columnName}, constraintName);
		}

		public static void AddForeignKey(this IQueryContext context, ObjectName table, string[] columns, ObjectName refTable,
			string[] refColumns, ForeignKeyAction deleteRule, ForeignKeyAction updateRule,
			String constraintName) {
			AddForeignKey(context, table, columns, refTable, refColumns, deleteRule, updateRule, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddForeignKey(this IQueryContext context, ObjectName table, string[] columns, ObjectName refTable,
			string[] refColumns, ForeignKeyAction deleteRule, ForeignKeyAction updateRule, ConstraintDeferrability deferred,
			String constraintName) {
			// TODO: throw a specialized exception
			if (!context.UserCanAlterTable(table))
				throw new InvalidOperationException();

			context.Session.AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns) {
			AddUniqueKey(context, tableName, columns, null);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns, string constraintName) {
			AddUniqueKey(context, tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferrability) {
			AddUniqueKey(context, tableName, columns, deferrability, null);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferrability, string constraintName) {
			if (!context.UserCanAlterTable(tableName))
				throw new InvalidOperationException();

			context.Session.AddUniqueKey(tableName, columns, deferrability, constraintName);
		}

		#endregion

		#endregion

		#region Views

		public static bool ViewExists(this IQueryContext context, ObjectName viewName) {
			return context.ObjectExists(DbObjectType.View, viewName);
		}

		public static void DefineView(this IQueryContext context, ViewInfo viewInfo) {
			var tablesInPlan = viewInfo.QueryPlan.DiscoverTableNames();
			foreach (var tableName in tablesInPlan) {
				if (!context.UserCanSelectFromTable(tableName))
					throw new InvalidOperationException(String.Format("User '{0}' cannot access the table '{1}' in the view query plan.", context.User(), tableName));
			}

			context.CreateObject(viewInfo);
		}

		public static View GetView(this IQueryContext context, ObjectName viewName) {
			return context.GetObject(DbObjectType.View, viewName) as View;
		}

		public static IQueryPlanNode GetViewQueryPlan(this IQueryContext context, ObjectName viewName) {
			var view = context.GetView(viewName);
			return view == null ? null : view.QueryPlan;
		}

		#endregion

		#region Triggers

		public static void FireTrigger(this IQueryContext context, TableEventContext tableEvent) {
			var eventSource = tableEvent.Table.FullName;
			var eventType = tableEvent.EventType;

			var triggers = context.Session.FindTriggers(eventSource, eventType);

			foreach (var trigger in triggers) {
				try {
					trigger.Fire(tableEvent);

					var oldRowId = tableEvent.OldRowId;
					var newRow = tableEvent.NewRow;

					var triggerEvent = new TriggerEvent(trigger.TriggerName, eventSource, eventType, oldRowId, newRow);

					context.RegisterEvent(triggerEvent);
				} catch (Exception) {
					// TODO: throw a specialized exception
					throw;
				}
			}
		}

		#endregion

		#region Sequences

		public static ISequence GetSequence(this IQueryContext context, ObjectName sequenceName) {
			return context.GetObject(DbObjectType.Sequence, sequenceName) as ISequence;
		}

		/// <summary>
		/// Increments the sequence and returns the computed value.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="sequenceName">The name of the sequence to increment and
		/// whose incremented value must be returned.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the result of
		/// the increment operation over the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public static SqlNumber GetNextValue(this IQueryContext context, ObjectName sequenceName) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			return sequence.NextValue();
		}

		/// <summary>
		/// Gets the current value of the sequence.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="sequenceName">The name of the sequence whose current value
		/// must be obtained.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the current value
		/// of the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public static SqlNumber GetCurrentValue(this IQueryContext context, ObjectName sequenceName) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			return sequence.GetCurrentValue();
		}

		/// <summary>
		/// Sets the current value of the sequence, overriding the increment
		/// mechanism in place.
		/// </summary>
		/// <param name="sequenceName">The name of the sequence whose current state
		/// to be set.</param>
		/// <param name="value">The numeric value to set.</param>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public static void SetCurrentValue(this IQueryContext context, ObjectName sequenceName, SqlNumber value) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			sequence.SetValue(value);
		}

		#endregion

		#region User Types

		public static UserType GetUserType(this IQueryContext context, ObjectName typeName) {
			return context.GetObject(DbObjectType.Type, typeName) as UserType;
		}

		#endregion

		#region Variables

		public static Variable GetVariable(this IQueryContext context, string variableName) {
			IQueryContext opContext = context;
			while (true) {
				if (opContext == null ||
					opContext.VariableManager == null)
					break;

				var variable = opContext.VariableManager.GetVariable(variableName);

				if (variable != null)
					return variable;
			}

			return null;
		}

		public static void SetVariable(this IQueryContext context, string variableName, DataObject value) {
			IQueryContext opContext = context;
			while (true) {
				if (opContext == null ||
					opContext.VariableManager == null)
					break;

				var variable = opContext.VariableManager.GetVariable(variableName);

				if (variable != null) {
					variable.SetValue(value);
					return;
				}
			}

			throw new InvalidOperationException(String.Format("Cannot find variable {0} in the context.", variableName));
		}

		public static void SetVariable(this IQueryContext context, ObjectName variableName, DataObject value) {
			context.SetVariable(variableName.Name, value);
		}

		#endregion
	}
}
