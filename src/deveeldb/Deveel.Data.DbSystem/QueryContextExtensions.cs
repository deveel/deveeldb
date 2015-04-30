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

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.DbSystem {
	public static class QueryContextExtensions {
		public static User User(this IQueryContext context) {
			return context.Session.SessionInfo.User;
		}

		public static IQueryContext ForSystemUser(this IQueryContext queryContext) {
			return new SystemQueryContext(queryContext.Session.Transaction, queryContext.CurrentSchema);
		}

		public static IDatabase Database(this IQueryContext context) {
			return context.Session.Database;
		}

		public static bool IgnoreIdentifiersCase(this IQueryContext context) {
			return context.Session.IgnoreIdentifiersCase();
		}

		#region Objects

		public static bool ObjectExists(this IQueryContext context, DbObjectType objectType, ObjectName objectName) {
			return context.Session.ObjectExists(objectType, objectName);
		}

		public static IDbObject GetObject(this IQueryContext context, DbObjectType objType, ObjectName objName) {
			return context.Session.GetObject(objType, objName);
		}

		public static ObjectName ResolveObjectName(this IQueryContext context, string name) {
			return context.Session.ResolveObjectName(name);
		}

		public static ObjectName ResolveObjectName(this IQueryContext context, DbObjectType objectType, ObjectName objectName) {
			return context.Session.ResolveObjectName(objectType, objectName);
		}

		#endregion

		#region Schemata

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
				context.CacheTable(tableName.FullName, table);
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

		#endregion

		#region Views

		public static View GetView(this IQueryContext context, ObjectName viewName) {
			return context.Session.GetView(viewName);
		}

		public static IQueryPlanNode GetViewQueryPlan(this IQueryContext context, ObjectName viewName) {
			return context.Session.GetViewQueryPlan(viewName);
		}

		#endregion

		#region Sequences

		public static ISequence GetSequence(this IQueryContext context, ObjectName sequenceName) {
			return context.Session.GetSequence(sequenceName);
		}

		/// <summary>
		/// Increments the sequence and returns the computed value.
		/// </summary>
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
