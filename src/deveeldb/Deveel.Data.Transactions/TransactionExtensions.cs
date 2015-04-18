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

using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Types;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// Provides some convenience extension methods to <see cref="ITransaction"/> instances.
	/// </summary>
	public static class TransactionExtensions {
		#region Managers

		public static TableManager GetTableManager(this ITransaction transaction) {
			return (TableManager) transaction.ObjectManagerResolver.ResolveForType(DbObjectType.Table);
		}

		public static ViewManager GetViewManager(this ITransaction transaction) {
			return (ViewManager) transaction.ObjectManagerResolver.ResolveForType(DbObjectType.View);
		}

		#endregion

		#region Objects

		public static IDbObject FindObject(this ITransaction transaction, ObjectName objName) {
			return transaction.ObjectManagerResolver.GetManagers()
				.Select(manager => manager.GetObject(objName))
				.FirstOrDefault(obj => obj != null);
		}

		public static IDbObject GetObject(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.ObjectManagerResolver.ResolveForType(objType);
			if (manager == null)
				return null;

			return manager.GetObject(objName);
		}

		public static bool ObjectExists(this ITransaction transaction, ObjectName objName) {
			return transaction.ObjectManagerResolver.GetManagers()
				.Any(manager => manager.ObjectExists(objName));
		}

		public static bool ObjectExists(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.ObjectManagerResolver.ResolveForType(objType);
			if (manager == null)
				return false;

			return manager.ObjectExists(objName);
		}

		public static bool RealObjectExists(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.ObjectManagerResolver.ResolveForType(objType);
			if (manager == null)
				return false;

			return manager.RealObjectExists(objName);			
		}

		public static void CreateObject(this ITransaction transaction, IObjectInfo objInfo) {
			if (objInfo == null)
				throw new ArgumentNullException("objInfo");

			var manager = transaction.ObjectManagerResolver.ResolveForType(objInfo.ObjectType);
			if (manager == null)
				throw new InvalidOperationException();

			if (manager.ObjectType != objInfo.ObjectType)
				throw new ArgumentException();

			manager.CreateObject(objInfo);
		}

		public static bool AlterObject(this ITransaction transaction, IObjectInfo objInfo) {
			if (objInfo == null)
				throw new ArgumentNullException("objInfo");

			var manager = transaction.ObjectManagerResolver.ResolveForType(objInfo.ObjectType);
			if (manager == null)
				throw new InvalidOperationException();

			if (manager.ObjectType != objInfo.ObjectType)
				throw new ArgumentException();

			return manager.AlterObject(objInfo);
		}

		public static bool DropObject(this ITransaction transaction, DbObjectType objType, ObjectName objName) {
			var manager = transaction.ObjectManagerResolver.ResolveForType(objType);
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

			foreach (var manager in transaction.ObjectManagerResolver.GetManagers()) {
				if (manager.ObjectExists(objName)) {
					if (found)
						throw new ArgumentException(String.Format("The name '{0}' is an ambigous match.", objectName));
 
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
			var manager = transaction.ObjectManagerResolver.ResolveForType(objectType);
			if (manager == null)
				return objectName;

			return manager.ResolveName(objectName, transaction.IgnoreIdentifiersCase());
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, ObjectName objectName) {
			var ignoreCase = transaction.IgnoreIdentifiersCase();

			return transaction.ObjectManagerResolver.GetManagers()
				.Select(manager => manager.ResolveName(objectName, ignoreCase))
				.FirstOrDefault(resolved => resolved != null);
		}

		#endregion

		#region Schema

		public static void CreateSchema(this ITransaction transaction, SchemaInfo schemaInfo) {
			transaction.CreateObject(schemaInfo);
		}

		public static void CreateSystemSchema(this ITransaction transaction) {
			SystemSchema.CreateSystemTables(transaction);

			// TODO: get the configured default culture...
			var culture = CultureInfo.CurrentCulture.Name;
			var schemaInfo = new SchemaInfo(SystemSchema.Name, SchemaTypes.System);
			schemaInfo.Culture = culture;

			transaction.CreateSchema(schemaInfo);
		}

		public static void DropSchema(this ITransaction transaction, string schemaName) {
			transaction.DropObject(DbObjectType.Schema, new ObjectName(schemaName));
		}

		public static Schema GetSchema(this ITransaction transaction, string schemaName) {
			var obj = transaction.GetObject(DbObjectType.Schema, new ObjectName(schemaName));
			if (obj == null)
				return null;

			return (Schema) obj;
		}

		public static bool SchemaExists(this ITransaction transaction, string schemaName) {
			return transaction.ObjectExists(DbObjectType.Schema, new ObjectName(schemaName));
		}

		#endregion

		#region Tables

		public static ObjectName ResolveReservedTableName(this ITransaction transaction, ObjectName tableName) {
			// We do not allow tables to be created with a reserved name
			var name = tableName.Name;

			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTableName;
			if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTableName;

			return tableName;
		}

		public static ObjectName ResolveTableName(this ITransaction transaction, ObjectName tableName) {
			// We do not allow tables to be created with a reserved name
			var name = tableName.Name;

			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTableName;
			if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTableName;

			return transaction.ResolveObjectName(DbObjectType.Table, tableName);
		}

		public static bool TableExists(this ITransaction transaction, ObjectName tableName) {
			return transaction.ObjectExists(DbObjectType.Table, transaction.ResolveReservedTableName(tableName));
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
			tableName = transaction.ResolveReservedTableName(tableName);
			if (tableName.Equals(SystemSchema.OldTriggerTableName, transaction.IgnoreIdentifiersCase()))
				return transaction.OldNewTableState.OldDataTable;
			if (tableName.Equals(SystemSchema.NewTriggerTableName, transaction.IgnoreIdentifiersCase()))
				return transaction.OldNewTableState.NewDataTable;

			var table = (ITable) transaction.GetObject(DbObjectType.Table, tableName);
			if (table != null) {
				//table = new DataTable(transaction, table);
				// TODO: encapsulate this into a table object that catches and fires events
			}

			return table;
		}

		internal static IEnumerable<TableSource> GetVisibleTables(this ITransaction transaction) {
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
			var tableManager = transaction.ObjectManagerResolver.ResolveForType(DbObjectType.Table) as TableManager;
			if (tableManager == null)
				throw new SystemException();

			return tableManager.GetTableInfo(tableName);
		}

		public static string GetTableType(this ITransaction transaction, ObjectName tableName) {
			var tableManager = transaction.GetTableManager();
			if (tableManager == null)
				throw new SystemException();

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

		#endregion

		#region Views

		public static void DefineView(this ITransaction transaction, ViewInfo viewInfo) {
			transaction.CreateObject(viewInfo);
		}

		public static IQueryPlanNode GetViewQueryPlan(this ITransaction transaction, ObjectName viewName) {
			var view = transaction.GetViewManager().GetView(viewName);
			if (view == null)
				return null;

			return view.QueryPlan;
		}

		#endregion

		#region Sequences

		public static void CreateSequence(this ITransaction transaction, SequenceInfo sequenceInfo) {
			transaction.CreateObject(sequenceInfo);
		}

		public static void CreateNativeSequence(this ITransaction transaction, ObjectName tableName) {
			var seqInfo = new SequenceInfo(tableName);
			transaction.CreateSequence(seqInfo);
		}

		public static void RemoveNativeSequence(this ITransaction transaction, ObjectName tableName) {
			transaction.DropSequence(tableName);
		}

		public static bool DropSequence(this ITransaction transaction, ObjectName sequenceName) {
			return transaction.DropObject(DbObjectType.Sequence, sequenceName);
		}

		public static ISequence GetSequence(this ITransaction transaction, ObjectName sequenceName) {
			return transaction.GetObject(DbObjectType.Sequence, sequenceName) as ISequence;
		}

		public static SqlNumber NextValue(this ITransaction transaction, ObjectName sequenceName) {
			var sequence = transaction.GetSequence(sequenceName);
			if (sequence == null)
				throw new ObjectNotFoundException(sequenceName);

			return sequence.NextValue();
		}

		public static SqlNumber LastValue(this ITransaction transaction, ObjectName sequenceName) {
			var sequence = transaction.GetSequence(sequenceName);
			if (sequence == null)
				throw new ObjectNotFoundException(sequenceName);

			return sequence.GetCurrentValue();
		}

		public static SqlNumber SetValue(this ITransaction transaction, ObjectName sequenceName, SqlNumber value) {
			var sequence = transaction.GetSequence(sequenceName);
			if (sequence == null)
				throw new ObjectNotFoundException(sequenceName);

			return sequence.SetValue(value);
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

		public static Variable DefineVariable(this ITransaction transaction, string name, DataType type) {
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

		#endregion

		#region Locks

		public static LockHandle LockRead(this ITransaction transaction, IEnumerable<ObjectName> tableNames, LockingMode mode) {
			var tables = tableNames.Select(transaction.GetTable).OfType<ILockable>();
			return transaction.Database.Context.Locker.Lock(new ILockable[0], tables.ToArray(), mode);
		}

		public static LockHandle LockWrite(this ITransaction transaction, IEnumerable<ObjectName> tableNames, LockingMode mode) {
			var tables = tableNames.Select(transaction.GetTable).OfType<ILockable>().ToArray();
			return transaction.Database.Context.Locker.Lock(tables, new ILockable[0], mode);
		}

		public static bool IsTableLocked(this ITransaction transaction, ITable table) {
			var lockable = table as ILockable;
			if (lockable == null)
				return false;

			return transaction.Database.Context.Locker.IsLocked(lockable);
		}

		#endregion
	}
}
