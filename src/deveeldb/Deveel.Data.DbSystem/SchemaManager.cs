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

using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class SchemaManager : IObjectManager {
		public SchemaManager(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;
		}

		~SchemaManager() {
			Dispose(false);
		}

		public ITransaction Transaction { get; private set; }

		private void Dispose(bool disposing) {
			if (disposing) {
				// TODO: Additional disposals ...
			}

			Transaction = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Schema; }
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var schemaInfo = objInfo as SchemaInfo;
			if (schemaInfo == null)
				throw new ArgumentException();

			CreateSchema(schemaInfo);
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return SchemaExists(objName.Name);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return SchemaExists(objName.Name);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetSchema(objName.Name);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			throw new NotImplementedException();
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropSchema(objName.Name);
		}

		public void CreateSchema(SchemaInfo schemaInfo) {
			if (schemaInfo == null)
				throw new ArgumentNullException("schemaInfo");

			var tableName = SystemSchema.SchemaInfoTableName;
			var t = Transaction.GetMutableTable(tableName);

			var nameObj = DataObject.String(schemaInfo.Name);

			if (t.Exists(1, nameObj))
				throw new DatabaseSystemException(String.Format("Schema '{0}' already defined in the database.", schemaInfo.Name));

			var row = t.NewRow();
			var uniqueId = Transaction.NextTableId(tableName);
			row.SetValue(0, DataObject.Number(uniqueId));
			row.SetValue(1, DataObject.String(schemaInfo.Name));
			row.SetValue(2, DataObject.String(schemaInfo.Type));
			row.SetValue(3, DataObject.String(schemaInfo.Culture));

			t.AddRow(row);
		}

		public bool SchemaExists(string name) {
			var tableName = SystemSchema.SchemaInfoTableName;
			var t = Transaction.GetMutableTable(tableName);

			var nameObj = DataObject.String(name);

			return t.Exists(1, nameObj);
		}

		public bool DropSchema(string name) {
			var tableName = SystemSchema.SchemaInfoTableName;
			var t = Transaction.GetMutableTable(tableName);

			// Drop a single entry from dt where column 1 = name
			var nameObj = DataObject.String(name);
			return t.Delete(1, nameObj);
		}

		public Schema GetSchema(string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			throw new NotImplementedException();
		}
	}
}