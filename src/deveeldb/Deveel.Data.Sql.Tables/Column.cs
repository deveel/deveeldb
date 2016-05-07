using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Index;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public sealed class Column : IDbObject, IEnumerable<Field> {
		public Column(ITable table, int offset) {
			if (table == null)
				throw new ArgumentNullException("table");
			if (offset < 0 || offset >= table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("offset");

			Table = table;
			Offset = offset;
		}

		public ITable Table { get; private set; }

		public int Offset { get; private set; }

		public string Name {
			get { return ColumnInfo.ColumnName; }
		}

		public ColumnInfo ColumnInfo {
			get { return Table.TableInfo[Offset]; }
		}

		public SqlType Type {
			get { return ColumnInfo.ColumnType; }
		}

		public ColumnIndex Index {
			get { return Table.GetIndex(Offset); }
		}

		IObjectInfo IDbObject.ObjectInfo {
			get { return ColumnInfo; }
		}

		public Field GetValue(long rowNumber) {
			return Table.GetValue(rowNumber, Offset);
		}

		public IEnumerator<Field> GetEnumerator() {
			return new ValueEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#region ValueEnumerator

		class ValueEnumerator : IEnumerator<Field> {
			private Column column;
			private int rowOffset = -1;
			private long rowCount;

			public ValueEnumerator(Column column) {
				this.column = column;
				rowCount = column.Table.RowCount;
			}

			~ValueEnumerator() {
				Dispose(false);
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			private void Dispose(bool disposing) {
				column = null;
			}

			public bool MoveNext() {
				return ++rowOffset < rowCount;
			}

			public void Reset() {
				rowOffset = -1;
				rowCount = column.Table.RowCount;
			}

			public Field Current {
				get { return column.GetValue(rowOffset); }
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}
