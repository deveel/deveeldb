using System;

using Deveel.Data.Select;

namespace Deveel.Data.DbModel {
	public interface ISqlStatementParser {
		SelectExpression ParseSelect(string sql);

		DbTable ParseCreateTable(string sql);
	}
}