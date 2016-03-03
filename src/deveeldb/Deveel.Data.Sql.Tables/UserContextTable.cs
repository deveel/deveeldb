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

using Deveel.Data.Index;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// A wrapper around a table that fires triggers on table events.
	/// </summary>
	class UserContextTable : BaseDataTable, IMutableTable {
		public UserContextTable(IQuery context, ITable table) {
			Context = context;
			Table = table;
		}

		public ITable Table { get; private set; }

		public IQuery Context { get; private set; }

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

		protected override int ColumnCount {
			get { return Table.ColumnCount(); }
		}

		private void OnTableEvent(TriggerEventType eventType, RowId rowId, Row row) {
			Context.FireTriggers(new TableEvent(this, eventType, rowId, row));
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			if (!TableName.Equals(ancestor.TableInfo.TableName) && 
				!ancestor.TableInfo.TableName.Equals(Table.TableInfo.TableName))
				throw new Exception("Method routed to incorrect table ancestor.");

			return rowSet;
		}

		protected override ColumnIndex GetColumnIndex(int columnOffset) {
			return Table.GetIndex(columnOffset);
		}

		protected override void SetupIndexes(Type indexType) {
		}

		public override Field GetValue(long rowNumber, int columnOffset) {
			return Table.GetValue(rowNumber, columnOffset);
		}

		public override void Lock() {
			if (IsMutable)
				MutableTable.AddLock();
		}

		public override void Release() {
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

		public RowId AddRow(Row row) {
			OnTableEvent(TriggerEventType.BeforeInsert, RowId.Null, row);
			
			var newRowId = MutableTable.AddRow(row);

			OnTableEvent(TriggerEventType.AfterInsert, newRowId, row);

			return newRowId;
		}

		public void UpdateRow(Row row) {
			if (row == null)
				throw new ArgumentNullException("row");

			var rowId = row.RowId;
			if (rowId.IsNull)
				throw new ArgumentException("Cannot update a row with NULL ROWID");

			OnTableEvent(TriggerEventType.BeforeUpdate, rowId, row);

			MutableTable.UpdateRow(row);
		}

		public bool RemoveRow(RowId rowId) {
			OnTableEvent(TriggerEventType.BeforeDelete, rowId, null);

			// TODO: Maybe we should return the row removed here
			var result = MutableTable.RemoveRow(rowId);

			OnTableEvent(TriggerEventType.AfterDelete, rowId, null);

			return result;
		}

		void IMutableTable.FlushIndexes() {
			MutableTable.FlushIndexes();
		}

		void IMutableTable.AssertConstraints() {
			MutableTable.AssertConstraints();
		}

		protected override void Dispose(bool disposing) {
			Context = null;
			Table = null;

			base.Dispose(disposing);
		}
	}
}
