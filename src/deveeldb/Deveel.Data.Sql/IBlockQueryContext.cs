using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	interface IBlockQueryContext {
		void Raise(string exceptionName);

		void ControlLoop(LoopControlType controlType, string label);

		void GoTo(string label);
	}
}
