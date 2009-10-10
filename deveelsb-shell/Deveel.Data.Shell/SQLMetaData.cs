using System;

using Deveel.Collections;

namespace Deveel.Data.Shell {
	public class SQLMetaData {
		public const int NOT_INITIALIZED = -1;

		private ISortedSet/*<Table>*/ _tables;

		public SQLMetaData() {
			_tables = new TreeSet();
		}

		public ISortedSet/*<Table>*/ Tables {
			get { return _tables; }
		}

		public void AddTable(Table table) {
			_tables.Add(table);
		}
	}
}