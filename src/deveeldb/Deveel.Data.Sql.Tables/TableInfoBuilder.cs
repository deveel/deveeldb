using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public class TableInfoBuilder {
		private string name;
		private string schema;
		private readonly List<ColumnInfoBuilder> columns;

		public TableInfoBuilder() {
			columns = new List<ColumnInfoBuilder>();
		}

		public TableInfoBuilder Named(ObjectName value) {
			if (value == null)
				throw new ArgumentNullException("value");

			name = value.Name;
			schema = value.ParentName;
			return this;
		}

		public TableInfoBuilder Named(string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			ObjectName fullName;
			if (!ObjectName.TryParse(value, out fullName))
				throw new ArgumentException();

			name = fullName.Name;
			schema = fullName.ParentName;

			return this;
		}

		public TableInfoBuilder InSchema(string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			schema = value;
			return this;
		}

		public TableInfoBuilder WithColumn(Action<ColumnInfoBuilder> column) {
			var builder = new ColumnInfoBuilder();
			column(builder);
			columns.Add(builder);

			return this;
		}

		public TableInfoBuilder WithColumn(string columnName, SqlType type) {
			return WithColumn(column => column.Named(columnName).HavingType(type));
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
