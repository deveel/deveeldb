using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.DbSystem {
	public interface IBlockQueryContext : IQueryContext {
		void SetReturn(SqlExpression expression);

		void Raise(string exceptionName);

		void ControlLoop(LoopControlType controlType, string label);

		void GoTo(string label);
	}
}
