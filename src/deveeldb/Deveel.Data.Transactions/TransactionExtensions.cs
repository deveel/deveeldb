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
using System.Globalization;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// Provides some convenience extension methods to <see cref="ITransaction"/> instances.
	/// </summary>
	public static class TransactionExtensions {
		#region Managers

		public static TableManager GetTableManager(this ITransaction transaction) {
			return (TableManager) transaction.ObjectManagerResolver.ResolveForType(DbObjectType.Table);
		}

		#endregion

		#region Objects

		public static IDbObject GetObject(this ITransaction transaction, ObjectName objName) {
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

		#endregion

		#region Schema

		public static void CreateSchema(this ITransaction transaction, SchemaInfo schemaInfo) {
			transaction.CreateObject(schemaInfo);
		}

		public static void CreateSystemSchema(this ITransaction transaction) {
			// TODO: get the configured default culture...
			var culture = CultureInfo.CurrentCulture.Name;
			var schemaInfo = new SchemaInfo(SystemSchema.Name, "SYSTEM");
			schemaInfo.Culture = culture;

			transaction.CreateSchema(schemaInfo);
			SystemSchema.Create(transaction);
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

		public static bool TableExists(this ITransaction transaction, ObjectName objName) {
			return transaction.ObjectExists(DbObjectType.Table, objName);
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
			return (ITable) transaction.GetObject(DbObjectType.Table, tableName);
		}

		public static IMutableTable GetMutableTable(this ITransaction transaction, ObjectName tableName) {
			return transaction.GetTable(tableName) as IMutableTable;
		}

		/// <summary>
		/// Creates a new table within this transaction with the given sector 
		/// size.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table already exists.
		/// </exception>
		public static void CreateTable(this ITransaction transaction, TableInfo tableInfo) {
			transaction.CreateObject(tableInfo);
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
	}
}
