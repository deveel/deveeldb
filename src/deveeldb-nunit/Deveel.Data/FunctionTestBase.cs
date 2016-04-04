using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class FunctionTestBase : ContextBasedTest {
		protected FunctionTestBase() {
		}

		protected FunctionTestBase(StorageType storageType)
			: base(storageType) {
		}

		protected Field Select(ObjectName functionName, params SqlExpression[] args) {
			return Query.SelectFunction(functionName, args);
		}

		protected Field Select(string functionName, params SqlExpression[] args) {
			return Select(new ObjectName(functionName), args);
		}
	}
}
