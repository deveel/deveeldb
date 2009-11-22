using System;
using System.Collections;

namespace Deveel.Data.Shell {
	public sealed class PrimaryKey {
		private readonly Table table;
		private readonly string name;
		private readonly ArrayList columns;

		internal PrimaryKey(Table table, string name) {
			this.table = table;
			this.name = name;
			columns = new ArrayList();
		}

		internal void AddColumn(String columnName) {
			if (!table.HasColumn(columnName))
				throw new InvalidOperationException();

			columns.Add(columnName);
		}

		public bool HasColumn(string column) {
			return columns.Contains(column);
		}

		public ICollection Columns {
			get { return columns; }
		}

		public string Name {
			get { return name; }
		}
	}
}