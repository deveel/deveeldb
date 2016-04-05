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

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public sealed class SqlExpressionParseException {
		[Test]
		public void SimpleNumericAdd() {
			const string s = "6578 + 76.32";

			SqlExpression sqlExp = null;
			Assert.DoesNotThrow(() => sqlExp = SqlExpression.Parse(s));
			Assert.IsNotNull(sqlExp);
			Assert.IsInstanceOf<SqlBinaryExpression>(sqlExp);
			Assert.AreEqual(SqlExpressionType.Add, sqlExp.ExpressionType);

			var binExp = (SqlBinaryExpression) sqlExp;
			Assert.IsInstanceOf<SqlConstantExpression>(binExp.Left);
			Assert.IsInstanceOf<SqlConstantExpression>(binExp.Right);
			Assert.IsTrue(binExp.CanEvaluate);
		}

		[Test]
		public void SimpleNumericSubtract() {
			const string s = "642.221 - 116.32";

			SqlExpression sqlExp = null;
			Assert.DoesNotThrow(() => sqlExp = SqlExpression.Parse(s));
			Assert.IsNotNull(sqlExp);
			Assert.IsInstanceOf<SqlBinaryExpression>(sqlExp);
			Assert.AreEqual(SqlExpressionType.Subtract, sqlExp.ExpressionType);

			var binExp = (SqlBinaryExpression) sqlExp;
			Assert.IsInstanceOf<SqlConstantExpression>(binExp.Left);
			Assert.IsInstanceOf<SqlConstantExpression>(binExp.Right);
			Assert.IsTrue(binExp.CanEvaluate);			
		}

		[Test]
		public void ChainedAddAndMultipleNumeric() {
			const string s = "75664 + 907 * 87";

			SqlExpression sqlExp = null;
			Assert.DoesNotThrow(() => sqlExp = SqlExpression.Parse(s));
			Assert.IsNotNull(sqlExp);
			Assert.IsInstanceOf<SqlBinaryExpression>(sqlExp);
			Assert.AreEqual(SqlExpressionType.Add, sqlExp.ExpressionType);

			var binExp = (SqlBinaryExpression) sqlExp;
			Assert.IsInstanceOf<SqlConstantExpression>(binExp.Left);
			Assert.IsInstanceOf<SqlBinaryExpression>(binExp.Right);
			Assert.IsTrue(binExp.CanEvaluate);

			SqlExpression evalExp = null;
			Assert.DoesNotThrow(() => evalExp = binExp.Evaluate());
			Assert.IsNotNull(evalExp);
			Assert.IsInstanceOf<SqlConstantExpression>(evalExp);

			var value = ((SqlConstantExpression) evalExp).Value;
			Assert.IsInstanceOf<SqlNumber>(value.Value);
			Assert.AreEqual(154573, ((SqlNumber)value.Value).ToInt32());
		}
	}
}