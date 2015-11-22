using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public interface IStatement : IExecutable, IPreparable {
		IStatement Prepare(IQuery context);
	}
}
