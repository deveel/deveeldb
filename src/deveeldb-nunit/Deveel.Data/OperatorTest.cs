using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class OperatorTest {
		[Test]
		public void AddNumerics() {
			TObject obj1 = 1, obj2 = 5;
			TObject result = Operator.Add.Evaluate(obj1, obj2);
			Assert.IsTrue(result == 6);
		}

		[Test]
		public void AddStrings() {
			TObject obj1 = "str1";
			TObject obj2 = "str2";
			TObject result = Operator.Add.Evaluate(obj1, obj2);
			Assert.IsTrue(result == "str1str2");
		}

		[Test]
		public void AddIntervalToDate() {
			TObject obj1 = new DateTime(2000, 12, 31);
			TObject obj2 = new TimeSpan(1, 0, 0, 0);
			TObject result = Operator.Add.Evaluate(obj1, obj2);
			Assert.IsTrue(result == new DateTime(2001, 1, 1));
		}

		[Test]
		public void SubtractNumerics() {
			TObject obj1 = 1, obj2 = 5;
			TObject result = Operator.Subtract.Evaluate(obj2, obj1);
			Assert.IsTrue(result == 4);
		}

		[Test]
		public void SubtractIntervalFromDate() {
			TObject obj1 = new DateTime(2001, 1, 1);
			TObject obj2 = new TimeSpan(1, 0, 0, 0);
			TObject result = Operator.Subtract.Evaluate(obj1, obj2);
			Assert.IsTrue(result == new DateTime(2000, 12, 31));
		}

		[Test]
		public void Concat() {
			TObject obj1 = "str1", obj2 = "str2";
			TObject result = Operator.Concat.Evaluate(obj1, obj2);
			Assert.IsTrue(result == "str1str2");
		}

		[Test]
		public void Multiply() {
			TObject obj1 = 22, obj2 = 3;
			TObject result = Operator.Multiply.Evaluate(obj1, obj2);
			Assert.IsTrue(result == 66);
		}
	}
}