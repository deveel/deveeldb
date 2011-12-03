using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class OperatorTest {
		[Test]
		public void Add() {
			// NUMERIC
			Operator op = Operator.Get("+");
			TObject obj1 = (TObject) 1, obj2 = (TObject) 5;
			TObject result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) 6);

			// STRING
			obj1 = (TObject) "str1";
			obj2 = (TObject) "str2";
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) "str1str2");

			// TIME + NUMERIC
			obj1 = (TObject) new DateTime(2000, 12, 31);
			obj2 = (TObject)(1*24*60*60*1000);
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) new DateTime(2001, 1, 1));

			// TIME + INTERVAL
			obj2 = (TObject) new TimeSpan(1, 0, 0,0);
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) new DateTime(2001, 1, 1));
		}

		[Test]
		public void Substract() {
			// NUMERIC
			Operator op = Operator.Get("-");
			TObject obj1 = (TObject) 1, obj2 = (TObject) 5;
			TObject result = op.Evaluate(obj2, obj1, null, null, null);
			Assert.IsTrue(result == (TObject) 4);

			// TIME - NUMERIC
			obj1 = (TObject) new DateTime(2001, 1, 1);
			obj2 = (TObject)(1*24*60*60*1000);
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) new DateTime(2000, 12, 31));

			// TIME - INTERVAL
			obj2 = (TObject) new TimeSpan(1, 0, 0, 0);
			result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) new DateTime(2000, 12, 31));
		}

		[Test]
		public void Concat() {
			Operator op = Operator.Get("||");
			TObject obj1 = (TObject) "str1", obj2 = (TObject) "str2";
			TObject result = op.Evaluate(obj1, obj2, null, null, null);
			Assert.IsTrue(result == (TObject) "str1str2");
		}
	}
}