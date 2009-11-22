using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbTableValues : ICollection {
		public DbTableValues(DbTable table) {
			this.table = table;
			values = new Hashtable();
		}

		private readonly Hashtable values;
		private readonly DbTable table;

		public object this[string columnName] {
			get { return GetValue(columnName); }
			set { SetValue(columnName, value); }
		}

		internal bool IsSet(string columnName) {
			return values.ContainsKey(columnName);
		}

		private void Set(ColumnValue value) {
			values[value.ColumnName] = value;
		}

		public void SetDefault(string columnName) {
			if (columnName == null)
				throw new ArgumentNullException("columnName");

			DbColumn column = table.GetColumn(columnName);
			if (column == null)
				throw new ArgumentException();

			if (column.Default == null || column.Default.Length == 0)
				throw new InvalidOperationException();

			values[columnName] = new ColumnValue(column, true);
		}

		public bool IsDefault(string columnName) {
			if (columnName == null)
				throw new ArgumentNullException("columnName");

			ColumnValue value = values[columnName] as ColumnValue;
			if (value == null) {
				DbColumn column = table.GetColumn(columnName);
				return column.Default != null && column.Default.Length > 0;
			}

			return value.Default;
		}

		public object GetValue(string columnName) {
			ColumnValue value = values[columnName] as ColumnValue;
			return (value == null ? null : value.Value);
		}

		public void SetValue(string columnName, object value) {
			if (columnName == null || columnName.Length == 0)
				throw new ArgumentNullException("columnName");

			DbColumn column = table.GetColumn(columnName);
			if (column == null)
				throw new InvalidOperationException();

			if (column.Identity)
				throw new InvalidOperationException();

			if ((value == null || value == DBNull.Value) &&
				!column.Nullable)
				throw new ArgumentException();

			values[columnName] = new ColumnValue(column, value);
		}

		#region Implementation of IEnumerable

		public IEnumerator GetEnumerator() {
			return new ValueEnumerator(values.GetEnumerator());
		}

		#endregion

		#region Implementation of ICollection

		void ICollection.CopyTo(Array array, int index) {
			if (index + values.Count > array.Length)
				throw new ArgumentException();

			ArrayList valuesList = new ArrayList(values.Values);
			for (int i = 0; i < valuesList.Count; i++) {
				ColumnValue value = (ColumnValue) valuesList[i];
				array.SetValue(value.Value, index + i);
			}
		}

		public int Count {
			get { return values.Count; }
		}

		object ICollection.SyncRoot {
			get { return values; }
		}

		bool ICollection.IsSynchronized {
			get { return values.IsSynchronized; }
		}

		#endregion

		#region ValueEnumerator

		private class ValueEnumerator : IEnumerator {
			public ValueEnumerator(IEnumerator en) {
				this.en = en;
			}

			private readonly IEnumerator en;

			#region Implementation of IEnumerator

			public bool MoveNext() {
				return en.MoveNext();
			}

			public void Reset() {
				en.Reset();
			}

			public object Current {
				get {
					ColumnValue value = en.Current as ColumnValue;
					return (value == null ? null : value.Value);
				}
			}

			#endregion
		}

		#endregion

		#region Value

		private class ColumnValue {
			private readonly DbColumn column;
			private readonly bool defaultValue;
			private readonly object value;

			public ColumnValue(DbColumn column, object value) {
				this.column = column;
				this.value = value;
			}

			public ColumnValue(DbColumn column, bool defaultValue)
				: this(column, null) {
				this.defaultValue = defaultValue;
			}

			public object Value {
				get { return defaultValue ? column.Default : value; }
			}

			public bool Default {
				get { return defaultValue; }
			}

			public string ColumnName {
				get { return column.Name; }
			}

			public static ColumnValue Null(DbColumn column) {
				return new ColumnValue(column, null);
			}
		}

		#endregion

		public static DbTableValues FromPrimaryKey(DbTable table) {
			DbTableValues values = new DbTableValues(table);

			DbConstraint[] constraints = table.GetConstraints(DbConstraintType.PrimaryKey);
			for (int i = 0; i < constraints.Length; i++) {
				DbPrimaryKey primaryKey = (DbPrimaryKey) constraints[i];
				ICollection columns = primaryKey.Columns;
				foreach(DbColumn column in columns) {
					values.Set(ColumnValue.Null(column));
				}
			}

			return values;
		}

		public static DbTableValues FromAll(DbTable table) {
			DbTableValues values = new DbTableValues(table);
			ICollection columns = table.Columns;
			foreach(DbColumn column in columns)
				values.Set(ColumnValue.Null(column));
			return values;
		}
	}
}