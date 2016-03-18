using System;

using Deveel.Data;
using Deveel.Data.Routines;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Spatial {
	[TestFixture]
	public class SpatialFunctionsTest : ContextBasedTest {
		protected override void RegisterServices(ServiceContainer container) {
			container.UseSpatial();
		}

		private Field ParseAndInvoke(string text) {
			var exp = SqlExpression.Parse(text);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(exp);

			var functionName = ((SqlFunctionCallExpression) exp).FunctioName;
			var args = ((SqlFunctionCallExpression) exp).Arguments;
			var invoke = new Invoke(functionName, args);

			return Query.Access.InvokeFunction(invoke);
		}

		[Test]
		public void PointFromWkt() {
			const string text = "FROM_WKT('POINT(50.100299 12.3399)')";

			var result = ParseAndInvoke(text);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			Assert.IsInstanceOf<SpatialType>(result.Type);

			var geometry = (SqlGeometry) result.Value;

			// TODO: Vrify strings equal to SqlString
			// Assert.AreEqual("POINT", geometry.GeometryType.ToString());
		}

		[Test]
		public void DistanceCalculate() {
			const string text = "DISTANCE(FROM_WKT('POINT(59.9308785 10.7893356)'), FROM_WKT('POINT(59.9284945 10.7786121)'))";

			var result = ParseAndInvoke(text);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);

			Assert.IsInstanceOf<NumericType>(result.Type);

			var number = (SqlNumber) result.Value;

			// TODO: Assess the distance is right
		}
	}
}
