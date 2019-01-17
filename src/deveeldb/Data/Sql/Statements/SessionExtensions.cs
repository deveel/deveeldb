using System;
using System.Threading.Tasks;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public static class SessionExtensions {
		public static Task<IStatementResult> ExecuteStatementAsync(this ISession session, SqlStatement statement) {
			if (statement.CanPrepare)
				statement = statement.Prepare(session);

			return statement.ExecuteAsync(session);
		}

		public static IStatementResult ExecuteStatement(this ISession session, SqlStatement statement) {
			return session.ExecuteStatementAsync(statement).Result;
		}

		#region Statements


		#region Create Table

		// TODO: missing the table create logic

		//public static Task CreateTableAsync(this ISession session, TableInfo tableInfo, bool temporary = false) {
		//	return session.ExecuteStatementAsync(new CreateTableStatement(tableInfo, temporary));
		//}

		//public static void CreateTable(this ISession session, TableInfo tableInfo, bool temporary = false) {
		//	session.CreateTableAsync(tableInfo, temporary).Wait();
		//}		

		#endregion

		#endregion
	}
}