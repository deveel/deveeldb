// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Diagnostics;
using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// Provides some convenience extension methods to <see cref="ITransaction"/> instances.
	/// </summary>
	public static class TransactionExtensions {
		private static void AssertNotReadOnly(this ITransaction transaction) {
			if (transaction.ReadOnly())
				throw new TransactionException(TransactionErrorCodes.ReadOnly, "The transaction is in read-only mode.");
		}

		public static IEventSource AsEventSource(this ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			var source = transaction as IEventSource;
			if (source != null)
				return source;

			return new EventSource(transaction.Context, transaction.Database.AsEventSource());
		}

		#region Managers

		public static TableManager GetTableManager(this ITransaction transaction) {
			return (TableManager) transaction.Context.ResolveService<IObjectManager>(DbObjectType.Table);
		}

		public static TriggerManager GetTriggerManager(this ITransaction transaction) {
			return transaction.Context.ResolveService<IObjectManager>(DbObjectType.Trigger) as TriggerManager;
		}

		#endregion

		#region Objects

		internal static IObjectManager GetObjectManager(this ITransaction transaction, DbObjectType objectType) {
			return transaction.Context.ResolveService<IObjectManager>(objectType);
		}

		private static IEnumerable<IObjectManager> GetObjectManagers(this ITransaction transaction) {
			return transaction.Context.ResolveAllServices<IObjectManager>();
		}

		public static IDbObject FindObject(this ITransaction transaction, ObjectName objName) {
			foreach (var manager in transaction.GetObjectManagers()) {
				if (manager.RealObjectExists(objName))
					return manager.GetObject(objName);
			}

			return null;
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

			return transaction.ResolveObjectName(objName);
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, string objectName) {
			var parsed = ObjectName.Parse(objectName);
			string schema;
			if (parsed.Parent != null) {
				schema = parsed.ParentName;
				objectName = parsed.Name;
			} else {
				schema = transaction.CurrentSchema();
			}

			return transaction.ResolveObjectName(schema, objectName);
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, DbObjectType objectType, ObjectName objectName) {
			var manager = transaction.GetObjectManager(objectType);
			if (manager == null)
				return objectName;

			if (objectName.Parent == null) {
				var currentSchema = transaction.CurrentSchema();
				objectName = new ObjectName(new ObjectName(currentSchema), objectName.Name);
			}

			var resolved = manager.ResolveName(objectName, transaction.IgnoreIdentifiersCase());
			if (resolved == null)
				resolved = objectName;

			return resolved;
		}

		public static ObjectName ResolveObjectName(this ITransaction transaction, ObjectName objectName) {
			var name = objectName.Name;

			// Special case for OLD and NEW tables
			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTableName;
			if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTableName;

			var ignoreCase = transaction.IgnoreIdentifiersCase();

			ObjectName found = null;
			foreach (var manager in transaction.GetObjectManagers()) {
				var resolved = manager.ResolveName(objectName, ignoreCase);

				if (resolved != null) {
					if (found != null)
						throw new ArgumentException(String.Format("The name '{0}' is an ambiguous match", objectName));

					found = resolved;
				}
			}

			if (found == null)
				return objectName;

			return found;
		}

		#endregion

		#region Schema

		public static void CreateSchema(this ITransaction transaction, SchemaInfo schemaInfo) {
			transaction.CreateObject(schemaInfo);
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
		/// Sets the current value of a table native sequence.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName">The table name.</param>
		/// <param name="value">The current value of the native sequence.</param>
		/// <seealso cref="ISequence"/>
		/// <seealso cref="SequenceManager"/>
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

		public static SqlNumber CurrentTableId(this ITransaction transaction, ObjectName tableName) {
			var tableManager = transaction.GetTableManager();
			if (tableManager == null)
				throw new InvalidOperationException();

			return tableManager.CurrentUniqueId(tableName);
		}

		#endregion

		#region Variables

		public static string CurrentSchema(this ITransaction transaction) {
			return transaction.Context.CurrentSchema();
		}

		public static void CurrentSchema(this ITransaction transaction, string value) {
			transaction.Context.CurrentSchema(value);
		}

		public static bool IgnoreIdentifiersCase(this ITransaction transaction) {
			return transaction.Context.IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this ITransaction transaction, bool value) {
			transaction.Context.IgnoreIdentifiersCase(value);
		}

		public static bool AutoCommit(this ITransaction transaction) {
			return transaction.Context.AutoCommit();
		}

		public static void AutoCommit(this ITransaction transaction, bool value) {
			transaction.Context.AutoCommit(value);
		}

		public static QueryParameterStyle ParameterStyle(this ITransaction transaction) {
			return transaction.Context.ParameterStyle();
		}

		public static void ParameterStyle(this ITransaction transaction, QueryParameterStyle value) {
			transaction.Context.ParameterStyle(value);
		}

		public static bool ReadOnly(this ITransaction transaction) {
			return transaction.Context.ReadOnly();
		}

		public static void ReadOnly(this ITransaction transaction, bool value) {
			transaction.Context.ReadOnly(value);
		}

		public static bool ErrorOnDirtySelect(this ITransaction transaction) {
			return transaction.Context.ErrorOnDirtySelect();
		}

		public static void ErrorOnDirtySelect(this ITransaction transaction, bool value) {
			transaction.Context.ErrorOnDirtySelect(value);
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

		#region Locks

		public static LockHandle Lock(this ITransaction transaction, IEnumerable<ObjectName> tableNames, AccessType accessType, LockingMode mode) {
			var lockables = tableNames.Select(transaction.FindObject).OfType<ILockable>();
			return transaction.Database.Locker.Lock(lockables.ToArray(), accessType, mode);
		}

		public static bool IsLocked(this ITransaction transaction, ITable table) {
			var lockable = table as ILockable;
			if (lockable == null)
				return false;

			return transaction.Database.Locker.IsLocked(lockable);
		}

		#endregion

		#region Events

		public static void OnObjectCreated(this ITransaction transaction, DbObjectType objectType, ObjectName objectName) {
			transaction.AsEventSource().OnEvent(new ObjectCreatedEvent(objectName, objectType));
		}

		public static void OnObjectDropped(this ITransaction transaction, DbObjectType objectType, ObjectName objectName) {
			transaction.AsEventSource().OnEvent(new ObjectDroppedEvent(objectType, objectName));
		}

		public static void OnTableCreated(this ITransaction transaction, int tableId, ObjectName tableName) {
			transaction.AsEventSource().OnEvent(new TableCreatedEvent(tableId, tableName));
		}

		public static void OnTableDropped(this ITransaction transaction, int tableId, ObjectName tableName) {
			transaction.AsEventSource().OnEvent(new TableDroppedEvent(tableId, tableName));
		}

		public static void OnTableAccessed(this ITransaction transaction, int tableId, ObjectName tableName) {
			transaction.AsEventSource().OnEvent(new TableAccessEvent(tableId, tableName));
		}

		public static void OnTableConstraintAltered(this ITransaction transaction, int tableId) {
			transaction.AsEventSource().OnEvent(new TableConstraintAlteredEvent(tableId));
		}

		#endregion
	}
}
