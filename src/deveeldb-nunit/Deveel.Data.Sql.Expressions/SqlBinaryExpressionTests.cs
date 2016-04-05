// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public sealed class SqlBinaryExpressionTests : SqlExpressionTestBase {
		[Test]
		public void NumericAndNumericAdd() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(4566, 10)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(8991.67, 10)));
			var addExp = SqlExpression.Add(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = addExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression) resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber) constExp.Value.Value).Round(2);
			var expected = new SqlNumber(13557.67, 2);
			
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void NumericAddToString() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(4566, 10)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(8991.67, 10)));
			var addExp = SqlExpression.Add(exp1, exp2);

			string s = null;
			Assert.DoesNotThrow(() => s = addExp.ToString());
			Assert.IsNotNull(s);
			Assert.IsNotEmpty(s);
			Assert.AreEqual("4566 + 8991.670000", s);
		}

		[Test]
		public void NumericAndBooleanAdd() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(4566)));
			var exp2 = SqlExpression.Constant(Field.Boolean(true));
			var addExp = SqlExpression.Add(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = addExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression) resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber) constExp.Value.Value).Round(2);
			var expected = new SqlNumber(4567, 2);
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void StringAndStringAdd() {
			var exp1 = SqlExpression.Constant(Field.String("The quick brown fox "));
			var exp2 = SqlExpression.Constant(Field.VarChar("jumps over the lazy dog"));
			var addExp = SqlExpression.Add(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = addExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression) resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<StringType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlString>(constExp.Value.Value);
			Assert.AreEqual(new SqlString("The quick brown fox jumps over the lazy dog"), (SqlString) constExp.Value.Value);
		}

		[Test]
		public void NumericAndNumericSubtract() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(879987.47, 10)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(2577.14, 10)));
			var subtractExp = SqlExpression.Subtract(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = subtractExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber) constExp.Value.Value).Round(2);
			var expected = new SqlNumber(877410.33, 2);

			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void NumericAndBooleanSubtract() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(325778.32)));
			var exp2 = SqlExpression.Constant(Field.Boolean(true));
			var subtractExp = SqlExpression.Subtract(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = subtractExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);
			Assert.AreEqual(new SqlNumber(325777.32), (SqlNumber)constExp.Value.Value);
		}

		[Test]
		public void NumericMultiply() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(56894.09)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(456)));
			var mulExp = SqlExpression.Multiply(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = mulExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber) constExp.Value.Value).Round(2);
			var expected = new SqlNumber(25943705.04, 2);
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void NumericDivide() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(49021.022)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(78.34)));
			var divExp = SqlExpression.Divide(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = divExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber)constExp.Value.Value).Round(5);
			var expected = new SqlNumber(625.74702, 5);
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void NumericModulo() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(892771.0623)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(9012)));
			var modExp = SqlExpression.Modulo(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = modExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber)constExp.Value.Value).Round(4);
			var expected = new SqlNumber(583.0623, 4);
			Assert.AreEqual(expected, actual);
		}

		[TestCase(9862711.650091, 9862711.650091, true)]
		[TestCase(12345, 2345, false)]
		[TestCase(123456.789, 123456.7, false)]
		public void NumericEqualTo(double a, double b, bool expected) {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(a)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(b)));
			var eqExp = SqlExpression.Equal(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = eqExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = (SqlBoolean) expected;
			Assert.AreEqual(expectedResult, actual);
		}

		[TestCase(763525.22e11, 763525.22e11, false)]
		[TestCase(12345, 123456, true)]
		[TestCase(564255.23899, 564255.23, true)]
		public void NumericNotEqualTo(double a, double b, bool expected) {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(a)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(b)));
			var eqExp = SqlExpression.NotEqual(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = eqExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = (SqlBoolean)expected;
			Assert.AreEqual(expectedResult, actual);
		}

		[TestCase(123998, 123999, false)]
		[TestCase(8764556.9011, 8764556.901145, false)]
		[TestCase(244591, 24620, true)]
		public void NumericGreaterThan(double a, double b, bool expected) {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(a)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(b)));
			var grExp = SqlExpression.GreaterThan(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = grExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = (SqlBoolean)expected;
			Assert.AreEqual(expectedResult, actual);
		}

		[TestCase(988271, 988271, false)]
		[TestCase(625.99e23, 12, false)]
		[TestCase(19283.9991e68, 19283.9991e69, true)]
		public void NumericSmallerThan(double a, double b, bool expected) {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(a)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(b)));
			var ltExp = SqlExpression.SmallerThan(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = ltExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = (SqlBoolean)expected;
			Assert.AreEqual(expectedResult, actual);
		}

		[TestCase(458849, 5526, 464375)]
		public void NumericOr(long a, long b, long expected) {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(a)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(b)));
			var orExp = SqlExpression.Or(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = orExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber)constExp.Value.Value);
			var expectedResult = new SqlNumber(expected);
			Assert.AreEqual(expectedResult, actual);			
		}

		[TestCase(true, true, true)]
		[TestCase(true, false, true)]
		[TestCase(false, false, false)]
		public void BooleanOr(bool a, bool b, bool expected) {
			var exp1 = SqlExpression.Constant(Field.Boolean(new SqlBoolean(a)));
			var exp2 = SqlExpression.Constant(Field.Boolean(new SqlBoolean(b)));
			var orExp = SqlExpression.Or(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = orExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = new SqlBoolean(expected);
			Assert.AreEqual(expectedResult, actual);
		}


		[TestCase(true, true, true)]
		[TestCase(true, false, false)]
		[TestCase(false, false, false)]
		public void BooleanAnd(bool a, bool b, bool expected) {
			var exp1 = SqlExpression.Constant(Field.Boolean(new SqlBoolean(a)));
			var exp2 = SqlExpression.Constant(Field.Boolean(new SqlBoolean(b)));
			var andExp = SqlExpression.And(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = andExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = new SqlBoolean(expected);
			Assert.AreEqual(expectedResult, actual);
		}


		[TestCase(567488, 90021, 653157)]
		public void NumericXOr(double a, double b, double expected) {
			var exp1 = SqlExpression.Constant(Field.Double(a));
			var exp2 = SqlExpression.Constant(Field.Double(b));
			var xorExp = SqlExpression.XOr(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = xorExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber)constExp.Value.Value);
			var expectedResult = new SqlNumber(expected);
			Assert.AreEqual(expectedResult, actual);			
		}

		[TestCase(6574493, 13324, 4108)]
		public void NumericAnd(long a, long b, long expected) {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(a)));
			var exp2 = SqlExpression.Constant(Field.Number(new SqlNumber(b)));
			var orExp = SqlExpression.And(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = orExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber)constExp.Value.Value);
			var expectedResult = new SqlNumber(expected);
			Assert.AreEqual(expectedResult, actual);
		}

		[Test]
		public void NumericIsNullTrue() {
			var exp1 = SqlExpression.Constant(Field.Number(SqlNumber.Null));
			var exp2 = SqlExpression.Constant(Field.Null());
			var orExp = SqlExpression.Is(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = orExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = new SqlBoolean(true);
			Assert.AreEqual(expectedResult, actual);			
		}

		[Test]
		public void NumericIsNullFalse() {
			var exp1 = SqlExpression.Constant(Field.Number(new SqlNumber(747748)));
			var exp2 = SqlExpression.Constant(Field.Null());
			var orExp = SqlExpression.Is(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = orExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = new SqlBoolean(false);
			Assert.AreEqual(expectedResult, actual);
		}

		[TestCase("Antonello", "Anto%", true)]
		[TestCase("Antonello", "Anto", false)]
		[TestCase("Antonello", "%nello", true)]
		[TestCase("Antonello", "Anto_ello", true)]
		[TestCase("Antonello", "Anton__ello", false)]
		[TestCase("Antonello", "%Antonello%", true)]
		[TestCase("Antonello", "Antonello_", false)]
		[TestCase("Antonello Provenzano", "Antonello%", true)]
		public void StringLikesPattern(string input, string patern, bool expected) {
			var exp1 = SqlExpression.Constant(Field.String(patern));
			var exp2 = SqlExpression.Constant(Field.String(input));
			var likeExp = SqlExpression.Like(exp1, exp2);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = likeExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = new SqlBoolean(expected);
			Assert.AreEqual(expectedResult, actual);
		}
	}
}