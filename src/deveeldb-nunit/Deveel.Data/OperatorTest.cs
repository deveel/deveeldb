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
			
			// TIME + INTERVAL
			obj1 = new DateTime(2000, 12, 31);
			obj2 = new TimeSpan(1, 0, 0,0);
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) new DateTime(2001, 1, 1));
		}

		[Test]
		public void Substract() {
			// NUMERIC
			Operator op = Operator.Get("-");
			TObject obj1 = 1, obj2 = 5;
			TObject result = op.Evaluate(obj2, obj1, null, null, null);
			Assert.IsTrue(result == 4);
			
			// TIME - INTERVAL
			obj1 = new DateTime(2001, 1, 1);
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

		[Test]
		public void Multiply() {
			Operator op = Operator.Multiply;
			TObject obj1 = 22, obj2 = 3;
			TObject result = op.Evaluate(obj1, obj2);
			Assert.AreEqual(66, result);
		}

		[Test]
		public void Divide() {
			Operator op = Operator.Divide;
			TObject obj1 = 127, obj2 = 3;
			TObject result = op.Evaluate(obj1, obj2);
			Assert.AreEqual(42.3333333333, result);
		}
	}
}