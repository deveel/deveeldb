using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class OperatorTest {
		[Test]
		public void Add() {
			Operator op = Operator.Get("=");
			TObject obj1 = 1, obj2 = 5;
			TObject result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == 6);
		}
	}
}