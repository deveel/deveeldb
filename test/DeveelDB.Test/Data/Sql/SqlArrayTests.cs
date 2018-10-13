using System;
using System.Collections;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlArrayTests {
		[Theory]
		[InlineData(40)]
		public static void ConstructArray(int length) {
			var type = new SqlArrayType(length);

			var array = new SqlArray(new SqlExpression[length]);
			Assert.Equal(length, array.Length);

			Assert.True(type.IsInstanceOf(array));
		}

		[Theory]
		[InlineData(30, 3)]
		public static void AccessItem(int length, int offset) {
			var exps = new SqlExpression[length];

			for (int i = 0; i < length; i++) {
				exps[i] = SqlExpression.Constant(new SqlObject(PrimitiveTypes.Integer(), (SqlNumber)i));
			}

			var array = new SqlArray(exps);

			Assert.Equal(length, exps.Length);
			Assert.NotNull(array[offset]);
			Assert.IsType<SqlConstantExpression>(array[offset]);
			Assert.IsType<SqlNumber>(((SqlConstantExpression) array[offset]).Value.Value);
			Assert.Equal(offset, (int)(SqlNumber) ((SqlConstantExpression) array[offset]).Value.Value);
		}

		[Theory]
		[InlineData(22, 25)]
		public static void CopyToExpressionsArray(int length, int expLength) {
			var exps = new SqlExpression[length];

			for (int i = 0; i < length; i++) {
				exps[i] = SqlExpression.Constant(new SqlObject(PrimitiveTypes.Integer(), (SqlNumber)i));
			}

			var array = new SqlArray(exps);

			var expArray = new SqlExpression[expLength];
			array.CopyTo(expArray, 0);

			Assert.NotNull(expArray[length-1]);
			Assert.Equal(array[length-1], expArray[length-1]);
		}

		[Theory]
		[InlineData(5, 16)]
		public static void CopyToOtherArray(int length1, int length2) {
			var exps = new SqlExpression[length1];

			for (int i = 0; i < length1; i++) {
				exps[i] = SqlExpression.Constant(new SqlObject(PrimitiveTypes.Integer(), (SqlNumber)i));
			}

			var array1 = new SqlArray(exps);
			var array2 = new SqlArray(new SqlExpression[length2]);

			array1.CopyTo(array2, 0);

			Assert.NotNull(array2[length1-1]);
			Assert.Equal(array1[length1-1], array2[length1-1]);
		}

		[Theory]
		[InlineData(5, "(0, 1, 2, 3, 4)")]
		[InlineData(0, "()")]
		[InlineData(1, "(0)")]
		public static void MakeAsString(int length, string expected) {
			var exps = new SqlExpression[length];

			for (int i = 0; i < length; i++) {
				exps[i] = SqlExpression.Constant(new SqlObject(PrimitiveTypes.Integer(), (SqlNumber)i));
			}

			var array = new SqlArray(exps);
			var s = array.ToString();

			Assert.Equal(expected, s);
		}

		[Theory]
		[InlineData(22)]
		public static void Enumerate(int length) {
			var exps = new SqlExpression[length];

			for (int i = 0; i < length; i++) {
				exps[i] = SqlExpression.Constant(new SqlObject(PrimitiveTypes.Integer(), (SqlNumber)i));
			}

			var array = new SqlArray(exps);

			var last = array.Last();
			Assert.NotNull(last);
			Assert.IsType<SqlConstantExpression>(last);
			Assert.Equal(length-1, (int)(SqlNumber) ((SqlConstantExpression)last).Value.Value);
		}

		[Theory]
		[InlineData(3)]
		public static void TryInvalidOperations(int length) {
			var array = new SqlArray(new SqlExpression[length]);
			Assert.Throws<NotSupportedException>(() => ((IList) array).Add(null));
			Assert.Throws<NotSupportedException>(() => ((IList) array).Remove(null));
			Assert.Throws<NotSupportedException>(() => ((IList) array).Clear());
			Assert.Throws<NotSupportedException>(() => ((IList) array).Insert(0, null));
		}
	}
}