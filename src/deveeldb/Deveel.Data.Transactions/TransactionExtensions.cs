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
using System.Globalization;
using System.Linq;

using Deveel.Data.Index;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Sql.Views;
using Deveel.Data.Types;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// Provides some convenience extension methods to <see cref="ITransaction"/> instances.
	/// </summary>
	public static class TransactionExtensions {
		private static void AssertNotReadOnly(this ITransaction transaction) {
			if (transaction.ReadOnly())
				throw new TransactionException(TransactionErrorCodes.ReadOnly, "The transaction is in read-only mode.");
		}

		#region Managers

		public static void CreateSystem(this ITransaction transaction) {
			var managers = transaction.TransactionContext.ResolveAllServices<IObjectManager>();
			foreach (var manager in managers) {
				manager.Create();
			}
		}

		public static TableManager GetTableManager(this ITransaction transaction) {
			return (TableManager) transaction.TransactionContext.ResolveService<IObjectManager>(DbObjectType.Table);
		}

		public static ViewManager GetViewManager(this ITransaction transaction) {
			return (ViewManager) transaction.TransactionContext.ResolveService<IObjectManager>(DbObjectType.View);
		}

		public static TriggerManager GetTriggerManager(this ITransaction transaction) {
			return transaction.TransactionContext.ResolveService<IObjectManager>(DbObjectType.Trigger) as TriggerManager;
		}

		#endregion

		#region Objects

		internal static IObjectManager GetObjectManager(this ITransaction transaction, DbObjectType objectType) {
			return transaction.TransactionContext.ResolveService<IObjectManager>(objectType);
		}

		private static IEnumerable<IObjectManager> GetObjectManagers(this ITransaction transaction) {
			return transaction.TransactionContext.ResolveAllServices<IObjectManager>();
		}

		public static IDbObject FindObject(this ITransaction transaction, ObjectName objName) {
			return transaction.GetObjectManagers()
				.Select(manager => manager.GetObject(objName))
				.FirstOrDefault(obj => obj != null);
		}

		public static IDbObject GetObject(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.GetObjectManager(objType);
			if (manager == null)
				return null;

			return manager.GetObject(objName);
		}

		public static bool ObjectExists(this ITransaction transaction, ObjectName objName) {
			return transaction.GetObjectManagers()
				.Any(manager => manager.ObjectExists(objName));
		}

		public static bool ObjectExists(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.GetObjectManager(objType);
			if (manager == null)
				return false;

			return manager.ObjectExists(objName);
		}

		public static bool RealObjectExists(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.GetObjectManager(objType);
			if (manager == null)
				return false;

			return manager.RealObjectExists(objName);			
		}

		public static void CreateObject(this ITransaction transaction, IObjectInfo objInfo) {
			if (objInfo == null)
				throw new ArgumentNullException("objInfo");

			var manager = transaction.GetObjectManager(objInfo.ObjectType);
			if (manager == null)
				throw new InvalidOperationException(String.Format("Could not find any manager for object type '{0}' configured for the system.", objInfo.ObjectType));

			if (manager.ObjectType != objInfo.ObjectType)
				throw new ArgumentException(
					String.Format("Could not create an object of type '{0}' with the manager '{1}' (supported '{2}' type)",
						objInfo.ObjectType, manager.GetType().FullName, manager.ObjectType));

			manager.CreateObject(objInfo);
		}

		public static bool AlterObject(this ITransaction transaction, IObjectInfo objInfo) {
			if (objInfo == null)
				throw new ArgumentNullException("objInfo");

			var manager = transaction.GetObjectManager(objInfo.ObjectType);
			if (manager == null)
				throw new InvalidOperationException();

			if (manager.ObjectType != objInfo.ObjectType)
				throw new ArgumentException();

			return manager.AlterObject(objInfo);
		}

		public static bool DropObject(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.GetObjectManager(objType);
			if (manager == null)
				return false;

			return manager.DropObject(objName);
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, string schemaName, string objectName) {
			if (String.IsNullOrEmpty(objectName))
				throw new ArgumentNullException("objectName");

			if (String.IsNullOrEmpty(schemaName))
				schemaName = transaction.CurrentSchema();

			var objName = new ObjectName(new ObjectName(schemaName), objectName);

			// Special case for OLD and NEW tables
			if (String.Compare(objectName, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTableName;
			if (String.Compare(objectName, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTableName;

			bool found = false;

			foreach (var manager in transaction.GetObjectManagers()) {
				if (manager.ObjectExists(objName)) {
					if (found)
						throw new ArgumentException(String.Format("The name '{0}' is an ambiguous match.", objectName));
 
					found = true;
				}
			}

			if (!found)
				throw new ObjectNotFoundException(objectName);

			return objName;
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, string objectName) {
			return transaction.ResolveObjectName(transaction.CurrentSchema(), objectName);
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, DbObjectType objectType, ObjectName objectName) {
			var manager = transaction.GetObjectManager(objectType);
			if (manager == null)
				return objectName;

			return manager.ResolveName(objectName, transaction.IgnoreIdentifiersCase());
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, ObjectName objectName) {
			var ignoreCase = transaction.IgnoreIdentifiersCase();

			return transaction.GetObjectManagers()
				.Select(manager => manager.ResolveName(objectName, ignoreCase))
				.FirstOrDefault(resolved => resolved != null);
		}

		#endregion

		#region Schema

		public static void CreateSchema(this ITransaction transaction, SchemaInfo schemaInfo) {
			transaction.CreateObject(schemaInfo);
		}

		// TODO: move this elsewhere
		public static void CreateSystemSchema(this ITransaction transaction) {
			transaction.CreateSystem();

			// TODO: get the configured default culture...
			var culture = CultureInfo.CurrentCulture.Name;
			var schemaInfo = new SchemaInfo(SystemSchema.Name, SchemaTypes.System);
			schemaInfo.Culture = culture;

			transaction.CreateSchema(schemaInfo);
		}

		#endregion

		#region Tables

		private static ObjectName ResolveReservedTableName(ObjectName tableName) {
			// We do not allow tables to be created with a reserved name
			var name = tableName.Name;

			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTableName;
			if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTableName;

			return tableName;
		}

		public static ObjectName ResolveTableName(this ITransaction transaction, ObjectName tableName) {
			if (tableName == null)
				return null;

			if (tableName.Parent == null)
				tableName = new ObjectName(new ObjectName(transaction.CurrentSchema()), tableName.Name);

			// We do not allow tables to be created with a reserved name
			var name = tableName.Name;

			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTableName;
			if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTableName;

			return transaction.ResolveObjectName(DbObjectType.Table, tableName);
		}

		public static bool TableExists(this ITransaction transaction, ObjectName tableName) {
			return transaction.ObjectExists(DbObjectType.Table, ResolveReservedTableName(tableName));
		}

		public static bool RealTableExists(this ITransaction transaction, ObjectName objName) {
			return transaction.RealObjectExists(DbObjectType.Table, objName);
		}

		/// <summary>
		/// Tries to get an object with the given name formed as table.
		/// </summary>
		/// <param name="transaction">The transaction object.</param>
		/// <param name="tableName">The name of the table to try to get.</param>
		/// <returns>
		/// Returns an instance of <see cref="ITable"/> if an object with the given name was
		/// found in the underlying transaction and it is of <see cref="DbObjectType.Table"/> and
		/// it is <c>not null</c>.
		/// </returns>
		public static ITable GetTable(this ITransaction transaction, ObjectName tableName) {
			tableName = ResolveReservedTableName(tableName);

			var tableStateHandler = transaction as ITableStateHandler;
			if (tableStateHandler != null) {
				if (tableName.Equals(SystemSchema.OldTriggerTableName, transaction.IgnoreIdentifiersCase()))
					return tableStateHandler.TableState.OldDataTable;
				if (tableName.Equals(SystemSchema.NewTriggerTableName, transaction.IgnoreIdentifiersCase()))
					return tableStateHandler.TableState.NewDataTable;
			}

			return (ITable) transaction.GetObject(DbObjectType.Table, tableName);
		}

		internal static IEnumerable<ITableSource> GetVisibleTables(this ITransaction transaction) {
			return transaction.GetTableManager().GetVisibleTables();
		}

		internal static void RemoveVisibleTable(this ITransaction transaction, TableSource table) {
			transaction.GetTableManager().RemoveVisibleTable(table);
		}

		internal static void UpdateVisibleTable(this ITransaction transaction, TableSource tableSource, IIndexSet indexSet) {
			transaction.GetTableManager().UpdateVisibleTable(tableSource, indexSet);
		}

		internal static IIndexSet GetIndexSetForTable(this ITransaction transaction, TableSource tableSource) {
			return transaction.GetTableManager().GetIndexSetForTable(tableSource);
		}

		public static IMutableTable GetMutableTable(this ITransaction transaction, ObjectName tableName) {
			return transaction.GetTable(tableName) as IMutableTable;
		}

		public static TableInfo GetTableInfo(this ITransaction transaction, ObjectName tableName) {
			var tableManager = transaction.GetObjectManager(DbObjectType.Table) as TableManager;
			if (tableManager == null)
				throw new InvalidOperationException("No table manager was found.");

			return tableManager.GetTableInfo(tableName);
		}

		public static string GetTableType(this ITransaction transaction, ObjectName tableName) {
			var tableManager = transaction.GetTableManager();
			if (tableManager == null)
				throw new InvalidOperationException("No table manager was found.");

			return tableManager.GetTableType(tableName);
		}

		public static void CreateTable(this ITransaction transaction, TableInfo tableInfo) {
			CreateTable(transaction, tableInfo, false);
		}

		public static void CreateTable(this ITransaction transaction, TableInfo tableInfo, bool temporary) {
			var tableManager = transaction.GetTableManager();
			if (temporary) {
				tableManager.CreateTemporaryTable(tableInfo);
			} else {
				tableManager.CreateTable(tableInfo);
			}
		}

		/// <summary>
		/// Alters the table with the given name within this transaction to the
		/// specified table definition.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public static void AlterTable(this ITransaction transaction, TableInfo tableInfo) {
			transaction.AlterObject(tableInfo);
		}

		public static bool DropTable(this ITransaction transaction, ObjectName tableName) {
			return transaction.DropObject(DbObjectType.Table, tableName);
		}

		/// <summary>
		/// Sets the current value of a table native sequence.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName">The table name.</param>
		/// <param name="value">The current value of the native sequence.</param>
		/// <seealso cref="ISequence"/>
		/// <seealso cref="ISequenceManager"/>
		/// <returns>
		/// Returns the current table sequence value after the set.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If it was not possible to find any table having the given
		/// <paramref name="tableName">name</paramref>.
		/// </exception>
		public static SqlNumber SetTableId(this ITransaction transaction, ObjectName tableName, SqlNumber value) {
			transaction.AssertNotReadOnly();

			var tableManager = transaction.GetTableManager();
			if (tableManager == null)
				throw new InvalidOperationException();

			return tableManager.SetUniqueId(tableName, value);
		}

		/// <summary>
		/// Gets the next value of a table native sequence.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <returns>
		/// Returns the next value of the sequence for the given table.
		/// </returns>
		public static SqlNumber NextTableId(this ITransaction transaction, ObjectName tableName) {
			transaction.AssertNotReadOnly();

			var tableManager = transaction.GetTableManager();
			if (tableManager == null)
				throw new InvalidOperationException();

			return tableManager.NextUniqueId(tableName);
		}

		#endregion

		#region Sequences

		public static void CreateSequence(this ITransaction transaction, SequenceInfo sequenceInfo) {
			transaction.CreateObject(sequenceInfo);
		}

		public static void CreateNativeSequence(this ITransaction transaction, ObjectName tableName) {
			transaction.CreateSequence(SequenceInfo.Native(tableName));
		}

		public static void RemoveNativeSequence(this ITransaction transaction, ObjectName tableName) {
			transaction.DropSequence(tableName);
		}

		public static bool DropSequence(this ITransaction transaction, ObjectName sequenceName) {
			return transaction.DropObject(DbObjectType.Sequence, sequenceName);
		}

		#endregion

		#region Variables

		public static Variable GetVariable(this ITransaction transaction, string name) {
			return transaction.GetObject(DbObjectType.Variable, new ObjectName(name)) as Variable;
		}

		public static void SetVariable(this ITransaction transaction, string name, DataObject value) {
			var variable = transaction.GetVariable(name);
			if (variable == null)
				variable = transaction.DefineVariable(name, value.Type);

			variable.SetValue(value);
		}

		public static void SetBooleanVariable(this ITransaction transaction, string name, bool value) {
			transaction.SetVariable(name, DataObject.Boolean(value));
		}

		public static void SetStringVariable(this ITransaction transaction, string name, string value) {
			transaction.SetVariable(name, DataObject.String(value));
		}

		public static Variable DefineVariable(this ITransaction transaction, string name, SqlType type) {
			var variableInfo = new VariableInfo(name, type, false);
			transaction.CreateObject(variableInfo);
			return transaction.GetVariable(name);
		}

		public static bool GetBooleanVariable(this ITransaction transaction, string name) {
			var variable = transaction.GetVariable(name);
			if (variable == null)
				return false;

			return variable.Value.AsBoolean();
		}

		public static string GetStringVariable(this ITransaction transaction, string name) {
			var variable = transaction.GetVariable(name);
			if (variable == null)
				return null;

			return variable.Value;
		}

		public static bool IgnoreIdentifiersCase(this ITransaction transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.IgnoreIdentifiersCase);
		}

		public static void IgnoreIdentifiersCase(this ITransaction transaction, bool value) {
			transaction.SetBooleanVariable(TransactionSettingKeys.IgnoreIdentifiersCase, value);
		}

		public static bool ReadOnly(this ITransaction transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.ReadOnly);
		}

		public static void ReadOnly(this ITransaction transaction, bool value) {
			transaction.SetBooleanVariable(TransactionSettingKeys.ReadOnly, value);
		}

		public static bool AutoCommit(this ITransaction transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.AutoCommit);
		}

		public static void AutoCommit(this ITransaction transaction, bool value) {
			transaction.SetBooleanVariable(TransactionSettingKeys.AutoCommit, value);
		}

		public static void CurrentSchema(this ITransaction transaction, string schemaName) {
			transaction.SetStringVariable(TransactionSettingKeys.CurrentSchema, schemaName);
		}

		public static string CurrentSchema(this ITransaction transaction) {
			return transaction.GetStringVariable(TransactionSettingKeys.CurrentSchema);
		}

		public static bool ErrorOnDirtySelect(this ITransaction transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.ErrorOnDirtySelect);
		}

		public static QueryParameterStyle ParameterStyle(this ITransaction transaction) {
			var styleString = transaction.GetStringVariable(TransactionSettingKeys.ParameterStyle);
			if (String.IsNullOrEmpty(styleString))
				return QueryParameterStyle.Default;

			return (QueryParameterStyle) Enum.Parse(typeof (QueryParameterStyle), styleString, true);
		}

		public static void ParameterStyle(this ITransaction transaction, QueryParameterStyle value) {
			if (value == QueryParameterStyle.Default)
				return;

			var styleString = value.ToString();
			transaction.SetStringVariable(TransactionSettingKeys.ParameterStyle, styleString);
		}

		public static void ParameterStyle(this ITransaction transaction, string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			var style = (QueryParameterStyle) Enum.Parse(typeof (QueryParameterStyle), value, true);
			transaction.ParameterStyle(style);
		}

		#endregion

		#region Locks

		public static LockHandle LockTables(this ITransaction transaction, IEnumerable<ObjectName> tableNames, AccessType accessType, LockingMode mode) {
			var tables = tableNames.Select(transaction.GetTable).OfType<ILockable>();
			return transaction.Database.Locker().Lock(tables.ToArray(), accessType, mode);
		}

		public static bool IsTableLocked(this ITransaction transaction, ITable table) {
			var lockable = table as ILockable;
			if (lockable == null)
				return false;

			return transaction.Database.Locker().IsLocked(lockable);
		}

		#endregion
	}
}
