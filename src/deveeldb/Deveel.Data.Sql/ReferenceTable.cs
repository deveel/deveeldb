using System;

namespace Deveel.Data.Sql {
	class ReferenceTable : FilterTable, IRootTable {
		private readonly TableInfo tableInfo;

		public ReferenceTable(ITable parent, ObjectName tableName) 
			: this(parent, parent.TableInfo.Alias(tableName)) {
		}

		public ReferenceTable(ITable parent, TableInfo tableInfo)
			: base(parent) {
			TableName = tableInfo.TableName;
			this.tableInfo = tableInfo;
		}

		public ObjectName TableName { get; private set; }

		public override TableInfo TableInfo {
			get { return tableInfo; }
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			var tableName = columnName.Parent;
			if (tableName != null && tableName.Equals(TableName))
				return TableInfo.IndexOfColumn(columnName.Name);

			return -1;
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			return new ObjectName(TableName, tableInfo[column].ColumnName);
		}

		public bool Equals(IRootTable other) {
			return other == this;
		}
	}
}
