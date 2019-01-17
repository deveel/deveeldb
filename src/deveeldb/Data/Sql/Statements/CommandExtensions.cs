using System;
using System.Threading.Tasks;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public static class CommandExtensions {
		public static async Task<IStatementResult> ExecuteStatementAsync(this ICommand command, SqlStatement statement) {
			if (statement.CanPrepare)
				statement = statement.Prepare(command);

			return await statement.ExecuteAsync(command);
		}
	}
}