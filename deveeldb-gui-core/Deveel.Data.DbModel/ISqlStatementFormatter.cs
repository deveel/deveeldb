using System;

using Deveel.Data.Select;

namespace Deveel.Data.DbModel {
	public interface ISqlStatementFormatter {
		string FormatColumnDeclaration(DbColumn column);

		string FormatConstraintDeclaration(DbConstraint constraint);

		string FormatCreateTable(DbTable table, bool ifNotExists);

		string FormatCreateView(string name, SelectExpression expression);

		string FormatInsert(DbTable table, DbTableValues values);

		string FormatDelete(DbTable table, DbTableValues values);

		string FormatUpdate(DbTable table, DbTableValues values);
	}
}