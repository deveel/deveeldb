using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class OperatorTest {
		[Test]
		public void Add() {
			// NUMERIC
			Operator op = Operator.Get("+");
			TObject obj1 = 1, obj2 = 5;
			TObject result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == 6);

			// STRING
			obj1 = "str1";
			obj2 = "str2";
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == "str1str2");

			// TIME + NUMERIC
			obj1 = new DateTime(2000, 12, 31);
			obj2 = 1*24*60*60*1000;
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == new DateTime(2001, 1, 1));

			// TIME + INTERVAL
			obj2 = new TimeSpan(1, 0, 0,0);
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == new DateTime(2001, 1, 1));
		}

		[Test]
		public void Substract() {
			// NUMERIC
			Operator op = Operator.Get("-");
			TObject obj1 = 1, obj2 = 5;
			TObject result = op.Evaluate(obj2, obj1, null, null, null);
			Assert.IsTrue(result == 4);

			// TIME - NUMERIC
			obj1 = new DateTime(2001, 1, 1);
			obj2 = 1*24*60*60*1000;
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == new DateTime(2000, 12, 31));

			// TIME - INTERVAL
			obj2 = new TimeSpan(1, 0, 0, 0);
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == new DateTime(2000, 12, 31));
		}

		[Test]
		public void Concat() {
			Operator op = Operator.Get("||");
			TObject obj1 = "str1", obj2 = "str2";
			TObject result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == "str1str2");
		}
	}
}