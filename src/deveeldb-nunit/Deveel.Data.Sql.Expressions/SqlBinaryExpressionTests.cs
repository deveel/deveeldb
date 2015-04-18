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
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public sealed class SqlBinaryExpressionTests {
		[Test]
		public void NumericAndNumericAdd() {
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(4566, 10)));
			var exp2 = SqlExpression.Constant(DataObject.Number(new SqlNumber(8991.67, 10)));
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
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(4566, 10)));
			var exp2 = SqlExpression.Constant(DataObject.Number(new SqlNumber(8991.67, 10)));
			var addExp = SqlExpression.Add(exp1, exp2);

			string s = null;
			Assert.DoesNotThrow(() => s = addExp.ToString());
			Assert.IsNotNullOrEmpty(s);
			Assert.AreEqual("4566 + 8991.670000", s);
		}

		[Test]
		public void NumericAndBooleanAdd() {
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(4566)));
			var exp2 = SqlExpression.Constant(DataObject.Boolean(true));
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
			var exp1 = SqlExpression.Constant(DataObject.String("The quick brown fox "));
			var exp2 = SqlExpression.Constant(DataObject.VarChar("jumps over the lazy dog"));
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
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(879987.47, 10)));
			var exp2 = SqlExpression.Constant(DataObject.Number(new SqlNumber(2577.14, 10)));
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
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(325778.32)));
			var exp2 = SqlExpression.Constant(DataObject.Boolean(true));
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
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(56894.09)));
			var exp2 = SqlExpression.Constant(DataObject.Number(new SqlNumber(456)));
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
	}
}