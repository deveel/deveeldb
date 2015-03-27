using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines.Fluid {
	public interface IAggregateFunctionConfiguration : IFunctionConfiguration {
		IAggregateFunctionConfiguration OnAfterAggregate(Func<ExecuteContext, DataObject, DataObject> afterAggregate);
	}
}