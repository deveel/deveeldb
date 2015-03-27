using System;

using Deveel.Data.Types;

namespace Deveel.Data.Routines.Fluid {
	public interface IFunctionConfiguration {
		IFunctionConfiguration Named(ObjectName name);

		IFunctionConfiguration WithAlias(ObjectName alias);

		IFunctionConfiguration WithParameter(Action<IFunctionParameterConfiguration> config);

		IAggregateFunctionConfiguration Aggregate();

		IFunctionConfiguration ReturnsType(Func<ExecuteContext, DataType> returns);

		IFunctionConfiguration WhenExecute(Func<ExecuteContext, ExecuteResult> execute);

		IFunctionConfiguration WhenExecute(Func<DataObject, DataObject, DataObject> execute);
	}
}
