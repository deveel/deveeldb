using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	class TableInfoBuilder : ITableInfoBuilder {
		private string name;
		private string schema;
		private readonly List<ColumnInfoBuilder> columns;

		public TableInfoBuilder() {
			columns = new List<ColumnInfoBuilder>();
		}

		public ITableInfoBuilder Named(string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			ObjectName fullName;
			if (!ObjectName.TryParse(value, out fullName))
				throw new ArgumentException();

			name = fullName.Name;

			if (String.IsNullOrEmpty(schema))
				schema = fullName.ParentName;

			return this;
		}

		public ITableInfoBuilder InSchema(string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			schema = value;
			return this;
		}

		public ITableInfoBuilder WithColumn(Action<IColumnInfoBuilder> column) {
			var builder = new ColumnInfoBuilder();
			column(builder);
			columns.Add(builder);

			return this;
		}

		public TableInfo Build() {
			if (String.IsNullOrEmpty(name))
				throw new InvalidOperationException("A name for the table is required");

			var tableInfo = new TableInfo(new ObjectName(new ObjectName(schema), name));

			foreach (var column in columns) {
				var columnInfo = column.Build();

				tableInfo.AddColumn(columnInfo);
			}

			return tableInfo;
		}
	}
}
