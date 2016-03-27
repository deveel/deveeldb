using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class FunctionTestBase : ContextBasedTest {
		protected Field Select(ObjectName functionName, params SqlExpression[] args) {
			var result = Query.SelectFunction(functionName, args);
			if (result.RowCount == 0)
				return Field.Null();

			if (result.RowCount > 1)
				throw new InvalidOperationException();

			return result.GetValue(0, 0);
		}

		protected Field Select(string functionName, params SqlExpression[] args) {
			return Select(new ObjectName(functionName), args);
		}
	}
}
