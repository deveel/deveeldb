using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	class ColumnInfoBuilder : IColumnInfoBuilder {
		private string name;
		private SqlType type;
		private bool notNull;
		private SqlExpression defaultExpression;
		private string indexType;

		public IColumnInfoBuilder Named(string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			name = value;
			return this;
		}

		public IColumnInfoBuilder HavingType(SqlType value) {
			if (value == null)
				throw new ArgumentNullException("value");

			type = value;
			return this;
		}

		public IColumnInfoBuilder NotNull(bool value = true) {
			notNull = value;
			return this;
		}

		public IColumnInfoBuilder WithDefault(SqlExpression expression) {
			defaultExpression = expression;
			return this;
		}

		public IColumnInfoBuilder WithIndex(string value) {
			indexType = value;
			return this;
		}

		public ColumnInfo Build() {
			return new ColumnInfo(name, type) {
				IsNotNull = notNull,
				IndexType = indexType,
				DefaultExpression = defaultExpression
			};
		}
	}
}
