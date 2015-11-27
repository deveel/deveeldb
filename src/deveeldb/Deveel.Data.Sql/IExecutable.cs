using System;

namespace Deveel.Data.Sql {
	public interface IExecutable {
		void Execute(ExecutionContext context);
	}
}
