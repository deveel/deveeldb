using System;

namespace Deveel.Data.Sql.Statements {
	interface IBlockQueryContext {
		void Raise(string exceptionName);

		void ControlLoop(LoopControlType controlType, string label);

		void GoTo(string label);
	}
}
