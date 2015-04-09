using System;

namespace Deveel.Data.Sql {
	public interface ITableVariableResolver : IVariableResolver {
		ITableVariableResolver ForRow(int rowIndex);
	}
}
