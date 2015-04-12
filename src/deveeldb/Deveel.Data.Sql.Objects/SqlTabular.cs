using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Index;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlTabular : ITable, ISqlObject {
		private ITable table;

		private SqlTabular(ITable table) {
			this.table = table;
		}

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public bool IsNull {
			get { return table == null; }
		}

		private void AssertNotNull() {
			if (table == null)
				throw new NullReferenceException("The object is null.");
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		public static SqlTabular From(ITable table) {
			return new SqlTabular(table);
		}

		ObjectName IDbObject.FullName {
			get {
				AssertNotNull();
				return table.FullName;
			}
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Table; }
		}

		public IEnumerator<Row> GetEnumerator() {
			AssertNotNull();
			return table.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Dispose() {
			table = null;
		}

		IDatabaseContext ITable.DatabaseContext {
			get {
				AssertNotNull();
				return table.DatabaseContext;
			}
		}

		TableInfo ITable.TableInfo {
			get {
				AssertNotNull();
				return table.TableInfo;
			}
		}

		public int RowCount {
			get {
				AssertNotNull();
				return table.RowCount;
			}
		}

		public DataObject GetValue(long rowNumber, int columnOffset) {
			AssertNotNull();
			return table.GetValue(rowNumber, columnOffset);
		}

		ColumnIndex ITable.GetIndex(int columnOffset) {
			AssertNotNull();
			return table.GetIndex(columnOffset);
		}
	}
}
