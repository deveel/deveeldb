using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Spatial {
	[TestFixture]
	public class SpatialFunctionsTest : ContextBasedTest {
		protected override ISystemContext CreateSystemContext() {
			var context = base.CreateSystemContext();
			context.UserSpatial();
			return context;
		}

		private DataObject ParseAndInvoke(string text) {
			var exp = SqlExpression.Parse(text);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(exp);

			var functionName = ((SqlFunctionCallExpression) exp).FunctioName;
			var args = ((SqlFunctionCallExpression) exp).Arguments;
			var invoke = new Invoke(functionName, args);

			return QueryContext.InvokeFunction(invoke);
		}

		[Test]
		public void PointFromWkt() {
			const string text = "FROM_WKT('POINT(50.100299 12.3399)')";

			DataObject result = null;
			Assert.DoesNotThrow(() => result = ParseAndInvoke(text));
			Assert.IsNotNull(result);

			Assert.IsInstanceOf<SpatialType>(result.Type);
		}
	}
}
