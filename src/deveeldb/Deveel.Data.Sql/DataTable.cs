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

using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql {
	class DataTable : BaseDataTable, IMutableTable {
		public DataTable(ITransaction transaction, ITable table) {
			Transaction = transaction;
			Table = table;
		}

		public ITable Table { get; private set; }

		public ITransaction Transaction { get; private set; }

		private IMutableTable MutableTable {
			get { return Table as IMutableTable; }
		}

		private bool IsMutable {
			get { return Table is IMutableTable; }
		}

		public override IEnumerator<Row> GetEnumerator() {
			return Table.GetEnumerator();
		}

		public override TableInfo TableInfo {
			get { return Table.TableInfo; }
		}

		public override int RowCount {
			get { return Table.RowCount; }
		}

		protected override ColumnIndex GetColumnIndex(int columnOffset) {
			return Table.GetIndex(columnOffset);
		}

		protected override void SetupIndexes(Type indexType) {
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			return Table.GetValue(rowNumber, columnOffset);
		}

		public override void LockRoot(int lockKey) {
			if (IsMutable)
				MutableTable.AddLock();
		}

		public override void UnlockRoot(int lockKey) {
			if (IsMutable)
				MutableTable.RemoveLock();
		}

		TableEventRegistry IMutableTable.EventRegistry {
			get { return MutableTable.EventRegistry; }
		}

		void IMutableTable.AddLock() {
			MutableTable.AddLock();
		}

		void IMutableTable.RemoveLock() {
			MutableTable.RemoveLock();
		}

		public void AddRow(Row row) {
			// TODO: Fire events ...
			
			MutableTable.AddRow(row);
		}

		public void UpdateRow(Row row) {
			// TODO: Fire events ...

			MutableTable.UpdateRow(row);
		}

		public bool RemoveRow(RowId rowId) {
			// TODO: Fire events ...

			return MutableTable.RemoveRow(rowId);
		}

		void IMutableTable.FlushIndexes() {
			MutableTable.FlushIndexes();
		}

		void IMutableTable.AssertConstraints() {
			MutableTable.AssertConstraints();
		}
	}
}
