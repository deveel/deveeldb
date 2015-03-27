using System;

namespace Deveel.Data.Routines.Fluid {
	public interface IProcedureConfiguration {
		IProcedureConfiguration Named(ObjectName name);

		IProcedureConfiguration WithParameter(Action<IProcedureParameterConfiguration> config);

		IProcedureConfiguration WhenExecute(Action<ExecuteContext> execute);
	}
}
