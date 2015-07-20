using System;
using System.Text;

namespace Deveel.Data.Sql.Statements {
	class StatementSqlStringBuilder {
		private StringBuilder sqlBuilder;

		public StatementSqlStringBuilder() {
			sqlBuilder = new StringBuilder();
		}

		public void Visit(SqlStatement statement) {
			throw new NotImplementedException();
		}

		public override string ToString() {
			return sqlBuilder.ToString();
		}
	}
}
