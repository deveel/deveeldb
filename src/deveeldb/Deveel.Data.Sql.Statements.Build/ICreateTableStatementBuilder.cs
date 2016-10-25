using System;

namespace Deveel.Data.Sql.Statements.Build {
	public interface ICreateTableStatementBuilder {
		ICreateTableStatementBuilder Named(ObjectName tableName);

		ICreateTableStatementBuilder WithColumn(SqlTableColumn column);

		ICreateTableStatementBuilder WithConstraint(SqlTableConstraint constraint);

		ICreateTableStatementBuilder IfNotExists(bool value = true);

		ICreateTableStatementBuilder Temporary(bool value = true);
	}
}
