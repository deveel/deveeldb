using System;

using Deveel.Data.Types;

namespace Deveel.Data.Routines.Fluid {
	public interface IFunctionParameterConfiguration {
		IFunctionParameterConfiguration Named(string name);

		IFunctionParameterConfiguration OfType(DataType type);

		IFunctionParameterConfiguration Unbounded(bool flag);
	}
}