using System;

namespace Deveel.Data.Sql.Statements {
	public interface IStatementCache {
		bool TryGet(string text, out SqlStatement[] statements);

		void Set(string text, SqlStatement[] statements);
	}
}
