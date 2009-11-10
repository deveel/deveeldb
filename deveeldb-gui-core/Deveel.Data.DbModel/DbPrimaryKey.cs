using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbPrimaryKey : DbConstraint {
		public DbPrimaryKey(string schema, string table, string name) 
			: base(schema, table, name, DbConstraintType.PrimaryKey) {
			columns = new ArrayList();
		}

		private readonly ArrayList columns;

		public DbColumn this[string name] {
			get {
				int index = FindColumnIndex(name);
				return (index == -1 ? null : this[index]);
			}
		}

		public DbColumn this[int index] {
			get { return columns[index] as DbColumn; }
		}

		public IList Columns {
			get { return (IList) columns.Clone(); }
		}

		private int FindColumnIndex(string name) {
			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];
				if (column.Name == name)
					return i;
			}

			return -1;
		}

		public bool HasColumn(string name) {
			return FindColumnIndex(name) != -1;
		}

		public void AddColumn(DbColumn column) {
			if (column == null)
				throw new ArgumentNullException("column");

			if (column.Schema != Schema ||
				column.TableName != Table)
				throw new ArgumentException();

			if (HasColumn(column.Name))
				throw new ArgumentException();

			columns.Add(column);
		}
	}
}