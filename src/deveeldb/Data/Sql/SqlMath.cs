// 
//  Copyright 2010-2017 Deveel
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
//


using System;

using Deveel.Math;

namespace Deveel.Data.Sql {
	public static class SqlMath {
		public static SqlNumber Add(SqlNumber a, SqlNumber b)
			=> Add(a, b, WiderPrecision(a, b));

		public static SqlNumber Add(SqlNumber a, SqlNumber b, int precision) {
			if (SqlNumber.IsNumber(a)) {
				if (SqlNumber.IsNumber(b)) {
					var context = new MathContext(precision);
					var result = BigMath.Add(a.innerValue, b.innerValue, context);

					return new SqlNumber(SqlNumber.NumericState.None, result);
				}

				return b;
			}

			return a;
		}

		public static SqlNumber Subtract(SqlNumber a, SqlNumber b)
			=> Subtract(a, b, WiderPrecision(a, b));

		public static SqlNumber Subtract(SqlNumber a, SqlNumber b, int precision) {
			if (SqlNumber.IsNumber(a)) {
				if (SqlNumber.IsNumber(b)) {
					var context = new MathContext(precision);
					var result = BigMath.Subtract(a.innerValue, b.innerValue, context);

					return new SqlNumber(SqlNumber.NumericState.None, result);
				}

				return new SqlNumber(b.InverseState(), null);
			}

			return a;
		}

		public static SqlNumber Divide(SqlNumber a, SqlNumber b)
			=> Divide(a, b, WiderPrecision(a, b));

		public static SqlNumber Divide(SqlNumber a, SqlNumber b, int precision) {
			if (SqlNumber.IsNumber(a)) {
				if (SqlNumber.IsNumber(b)) {
					BigDecimal divBy = b.innerValue;
					if (divBy.CompareTo(BigDecimal.Zero) != 0) {
						var context = new MathContext(precision);
						var result = BigMath.Divide(a.innerValue, divBy, context);
						return new SqlNumber(SqlNumber.NumericState.None, result);
					}
					throw new DivideByZeroException();
				}
			}

			// Return NaN if we can't divide
			return SqlNumber.NaN;
		}

		private static int WiderPrecision(SqlNumber a, SqlNumber b) {
			return WiderPrecision(a.Precision, b.Precision);
		}

		private static int WiderPrecision(int a, int b) {
			if (a > b)
				return a;
			if (b > a)
				return b;

			return a;
		}


		public static SqlNumber Multiply(SqlNumber a, SqlNumber b) {
			if (SqlNumber.IsNumber(a)) {
				if (SqlNumber.IsNumber(b)) {
					var result = BigMath.Multiply(a.innerValue, b.innerValue);
					return new SqlNumber(SqlNumber.NumericState.None, result);
				}

				return b;
			}

			return a;
		}

		public static SqlNumber Remainder(SqlNumber a, SqlNumber b) {
			if (SqlNumber.IsNumber(a)) {
				if (SqlNumber.IsNumber(b)) {
					BigDecimal divBy = b.innerValue;
					if (divBy.CompareTo(BigDecimal.Zero) != 0) {
						var remainder = BigMath.Remainder(a.innerValue, divBy);
						return new SqlNumber(SqlNumber.NumericState.None, remainder);
					}
				}
			}

			return SqlNumber.NaN;
		}

		public static SqlNumber Pow(SqlNumber number, SqlNumber exp) {
			if (SqlNumber.IsNumber(number)) {
				var result = BigMath.Pow(number.innerValue, exp.innerValue);
				return new SqlNumber(result);
			}

			return number;
		}

		public static SqlNumber Sqrt(SqlNumber number) {
			if (SqlNumber.IsNumber(number)) {
				return DoubleOperation(number, System.Math.Sqrt);
			}

			return number;
		} 

		public static SqlNumber Round(SqlNumber number) {
			return Round(number, number.MathContext.Precision);
		}

		public static SqlNumber Round(SqlNumber value, int precision) {
			if (SqlNumber.IsNumber(value)) {
				var result = BigMath.Round(value.innerValue, new MathContext(precision, RoundingMode.HalfUp));
				return new SqlNumber(result);
			}

			return value;
		}

		private static SqlNumber DoubleOperation(SqlNumber number, Func<double, double> op) {
			if (!SqlNumber.IsNumber(number))
				return number;

			var value = (double) number;
			var result = op(value);

			if (Double.IsNaN(result))
				return SqlNumber.NaN;
			if (Double.IsPositiveInfinity(result))
				return SqlNumber.PositiveInfinity;
			if (Double.IsNegativeInfinity(result))
				return SqlNumber.NegativeInfinity;

			return (SqlNumber)result;
		}

		public static SqlNumber Log(SqlNumber number) {
			return DoubleOperation(number, System.Math.Log);
		}

		public static SqlNumber Log(SqlNumber number, SqlNumber newBase) {
			if (SqlNumber.IsNumber(number))
				return (SqlNumber)System.Math.Log((double)number, (double) newBase);

			return number;
		}

		public static SqlNumber Cos(SqlNumber number) {
			return DoubleOperation(number, System.Math.Cos);
		}

		public static SqlNumber CosH(SqlNumber number) {
			return DoubleOperation(number, System.Math.Cosh);
		}

		public static SqlNumber Tan(SqlNumber number) {
			return DoubleOperation(number, System.Math.Tan);
		}

		public static SqlNumber TanH(SqlNumber number) {
			return DoubleOperation(number, System.Math.Tanh);
		}

		public static SqlNumber Sin(SqlNumber number) {
			return DoubleOperation(number, System.Math.Sin);
		}

		public static SqlNumber Abs(SqlNumber number) {
			if (SqlNumber.IsNumber(number))
				return new SqlNumber(SqlNumber.NumericState.None, BigMath.Abs(number.innerValue));
			if (SqlNumber.IsNegativeInfinity(number))
				return SqlNumber.PositiveInfinity;

			return number;
		}
	}
}