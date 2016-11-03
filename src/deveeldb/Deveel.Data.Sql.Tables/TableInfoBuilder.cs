// 
//  Copyright 2010-2016 Deveel
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
