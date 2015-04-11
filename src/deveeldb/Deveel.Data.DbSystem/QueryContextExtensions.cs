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

namespace Deveel.Data.DbSystem {
	public static class QueryContextExtensions {
		public static User User(this IQueryContext context) {
			return context.Session.User;
		}

		public static IDatabase Database(this IQueryContext context) {
			return context.Session.Database;
		}

		public static bool ObjectExists(this IQueryContext context, DbObjectType objectType, ObjectName objectName) {
			return context.Session.ObjectExists(objectType, objectName);
		}

		public static IDbObject GetObject(this IQueryContext context, DbObjectType objType, ObjectName objName) {
			return context.Session.GetObject(objType, objName);
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

		public static ISequence GetSequence(this IQueryContext context, ObjectName sequenceName) {
			return context.Session.GetSequence(sequenceName);
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

		#region Security

		public static bool UserHasPrivilege(this IQueryContext context, DbObjectType objectType, ObjectName objectName,
			Privileges privileges) {
			return context.Session.UserHasPrivilege(objectType, objectName, privileges);
		}

		public static bool UserCanCreateInSchema(this IQueryContext context, string schemaName) {
			return context.UserHasPrivilege(DbObjectType.Schema, new ObjectName(schemaName), Privileges.Create);
		}

		public static bool UserCanCreateInSchema(this IQueryContext context) {
			return context.UserCanCreateInSchema(context.CurrentSchema);
		}

		public static bool UserCanExecute(this IQueryContext context, RoutineType routineType, ObjectName routineName) {
			var objectType = routineType == RoutineType.Procedure ? DbObjectType.Procedure : DbObjectType.Function;
			return context.UserHasPrivilege(objectType, routineName, Privileges.Execute);
		}

		public static bool UserCanExecuteFunction(this IQueryContext context, ObjectName functionName) {
			return context.UserCanExecute(RoutineType.Function, functionName);
		}

		public static bool UserCanExecuteProcedure(this IQueryContext context, ObjectName procedureName) {
			return context.UserCanExecute(RoutineType.Procedure, procedureName);
		}

		#endregion

		#region Variables

		public static void SetVariable(this IQueryContext context, string variableName, DataObject value) {
			IQueryContext opContext = context;
			Variable variable = null;
			while (true) {
				if (opContext == null ||
					opContext.VariableManager == null)
					break;

				variable = opContext.VariableManager.GetVariable(variableName);

				if (variable != null) {
					variable.SetValue(value);
					return;
				}
			}

			throw new InvalidOperationException(String.Format("Cannot find variable {0} in the context.", variableName));
		}

		#endregion
	}
}
