// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	public sealed class TableBuilder {
		private readonly ICollection<ITableConfiguration> tableConfigs = new Collection<ITableConfiguration>();

		public TableBuilder CreateTable(Action<ITableConfiguration> config) {
			var tableConfig = new TableConfiguration();
			config(tableConfig);
			tableConfigs.Add(tableConfig);
			return this;
		}

		// TODO: Add methods to flush to a system

		#region TableConfiguration

		class TableConfiguration : ITableConfiguration {
			public TableConfiguration() {
				ColumnConfigs = new Collection<IColumnConfiguration>();
				ConstraintConfigs = new Collection<IConstraintConfiguration>();
			}

			public ObjectName TableName { get; private set; }

			public ICollection<IColumnConfiguration> ColumnConfigs { get; private set; }

			public ICollection<IConstraintConfiguration> ConstraintConfigs { get; private set; }

			public ITableConfiguration Named(ObjectName name) {
				TableName = name;
				return this;
			}

			public ITableConfiguration WithColumn(Action<IColumnConfiguration> columnConfig) {
				var config = new ColumnConfiguration();
				columnConfig(config);
				ColumnConfigs.Add(config);
				return this;
			}

			public ITableConfiguration WithConstraint(Action<IConstraintConfiguration> config) {
				var constrConfig = new ConstraintConfiguration();
				config(constrConfig);
				ConstraintConfigs.Add(constrConfig);
				return this;
			}
		}

		#endregion

		#region ConstraintConfiguration

		class ConstraintConfiguration : IConstraintConfiguration {
			public IConstraintConfiguration Named(string name) {
				throw new NotImplementedException();
			}

			public IConstraintConfiguration OnColumns(IEnumerable<string> columnNames) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region ColumnConfiguration

		class ColumnConfiguration : IColumnConfiguration {
			public string ColumnName { get; private set; }

			public DataType ColumnType { get; private set; }

			public bool NotNull { get; private set; }

			public SqlExpression DefaultExpression { get; private set; }

			public IColumnConfiguration Named(string columnName) {
				if (String.IsNullOrEmpty(columnName))
					throw new ArgumentNullException("columnName");

				ColumnName = columnName;
				return this;
			}

			public IColumnConfiguration TypeOf(DataType type) {
				if (type == null)
					throw new ArgumentNullException("type");

				ColumnType = type;
				return this;
			}

			public IColumnConfiguration IsNotNull(bool flag) {
				NotNull = flag;
				return this;
			}

			public IColumnConfiguration DefaultTo(SqlExpression expression) {
				DefaultExpression = expression;
				return this;
			}
		}

		#endregion
	}
}