using System;

using Deveel.Data.Types;

namespace Deveel.Data.Routines.Fluid {
	public interface IProcedureParameterConfiguration {
		IProcedureParameterConfiguration Named(string name);

		IProcedureParameterConfiguration WithDirection(ParameterDirection direction);

		IProcedureParameterConfiguration OfType(DataType dataType);
	}
}