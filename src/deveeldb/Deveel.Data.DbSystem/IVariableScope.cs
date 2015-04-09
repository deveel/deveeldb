using System;

using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	public interface IVariableScope : IVariableResolver {
		void OnVariableDefined(Variable variable);

		void OnVariableDropped(Variable variable);

		Variable OnVariableGet(string name);
	}
}
