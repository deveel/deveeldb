using System;

namespace Deveel.Data.Sql.Variables {
	public interface IVariableManager : IVariableResolver, IDisposable {
		Variable DefineVariable(VariableInfo variableInfo);

		bool VariableExists(string variableName);

		bool DropVariable(string variableName);

		Variable GetVariable(string variableName);
	}
}
