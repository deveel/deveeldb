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
using System.IO;

using Deveel.Math;

namespace Deveel.Data.Sql.Types {
	public class SqlNumericType : SqlType {
		internal const int TinyIntPrecision = 3;
		internal const int SmallIntPrecision = 5;
		internal const int IntegerPrecision = 10;
		internal const int BigIntPrecision = 19;
		internal const int DoublePrecision = 16;
		internal const int FloatPrecision = 8;
		internal const int DecimalPrecision = 24;

		public SqlNumericType(SqlTypeCode typeCode, int precision, int scale)
			: base(typeCode) {
			AssertIsNumeric(typeCode);

			AssertScale(typeCode, scale);

			if (precision < 0)
				precision = DiscoverPrecision(typeCode);

			ValidatePrecision(precision);

			Precision = precision;
			Scale = scale;
		}

		public int Precision { get; }

		public int Scale { get; }

		private static void AssertIsNumeric(SqlTypeCode typeCode) {
			if (!IsNumericType(typeCode))
				throw new ArgumentException($"The type '{typeCode}' is not a valid NUMERIC type.");
		}

		private static void AssertScale(SqlTypeCode typeCode, int scale) {
			switch (typeCode) {
				case SqlTypeCode.TinyInt:
				case SqlTypeCode.SmallInt:
				case SqlTypeCode.Integer:
				case SqlTypeCode.BigInt:
					if (scale > 0)
						throw new ArgumentException($"Integer type {typeCode} must have a scale of 0");

					break;
				case SqlTypeCode.Numeric:
					if (scale <= 0) {
						throw new ArgumentException("The NUMERIC type requires an explicit scale");
					}

					break;
			}
		}

		private static int DiscoverPrecision(SqlTypeCode typeCode) {
			switch (typeCode) {
				case SqlTypeCode.TinyInt:
					return TinyIntPrecision;
				case SqlTypeCode.SmallInt:
					return SmallIntPrecision;
				case SqlTypeCode.Integer:
					return IntegerPrecision;
				case SqlTypeCode.BigInt:
					return BigIntPrecision;
				case SqlTypeCode.Float:
				case SqlTypeCode.Real:
					return FloatPrecision;
				case SqlTypeCode.Double:
					return DoublePrecision;
				case SqlTypeCode.Decimal:
					return DecimalPrecision;
				case SqlTypeCode.VarNumeric:
					return-1;
				default:
					throw new ArgumentException($"Type {typeCode} requires an explicit precision");
			}
		}

		private void ValidatePrecision(int value) {
			bool valid;

			switch (TypeCode) {
				case SqlTypeCode.TinyInt:
					valid = value == TinyIntPrecision;
					break;
				case SqlTypeCode.SmallInt:
					valid = value == SmallIntPrecision;
					break;
				case SqlTypeCode.Integer:
					valid = value == IntegerPrecision;
					break;
				case SqlTypeCode.BigInt:
					valid = value == BigIntPrecision;
					break;
				case SqlTypeCode.Float:
					valid = value == FloatPrecision;
					break;
				case SqlTypeCode.Double:
					valid = value == DoublePrecision;
					break;
				case SqlTypeCode.Decimal:
					valid = value == DecimalPrecision;
					break;
				default:
					valid = true;
					break;
			}

			if (!valid)
				throw new ArgumentException($"The precision {value} is invalid for type {TypeCode}");
		}

		internal static bool IsNumericType(SqlTypeCode typeCode) {
			return typeCode == SqlTypeCode.TinyInt ||
			       typeCode == SqlTypeCode.SmallInt ||
			       typeCode == SqlTypeCode.Integer ||
			       typeCode == SqlTypeCode.BigInt ||
			       typeCode == SqlTypeCode.Real ||
			       typeCode == SqlTypeCode.Float ||
			       typeCode == SqlTypeCode.Double ||
			       typeCode == SqlTypeCode.Decimal ||
			       typeCode == SqlTypeCode.Numeric ||
				   typeCode == SqlTypeCode.VarNumeric;
		}

		public override bool IsInstanceOf(ISqlValue value) {
			if (value is SqlNumber) {
				var number = (SqlNumber) value;
				switch (TypeCode) {
					case SqlTypeCode.Integer:
					case SqlTypeCode.TinyInt:
					case SqlTypeCode.SmallInt:
					case SqlTypeCode.BigInt:
						return number.Scale == 0 && number.Precision <= Precision;
					case SqlTypeCode.Double:
					case SqlTypeCode.Float:
						return number.Precision <= Precision;
					case SqlTypeCode.Numeric:
						return number.Precision <= Precision &&
						       (Scale < 0 || number.Scale <= Scale);
					case SqlTypeCode.VarNumeric:
						return number.Precision > 0 && number.Scale >= 0;
				}
			}

			return value is SqlNull;
		}

		public override SqlBoolean Greater(ISqlValue a, ISqlValue b) {
			return Compare(a, b) > 0;
		}

		public override SqlBoolean GreaterOrEqual(ISqlValue a, ISqlValue b) {
			return Compare(a, b) >= 0;
		}

		public override SqlBoolean Less(ISqlValue a, ISqlValue b) {
			return Compare(a, b) < 0;
		}

		public override SqlBoolean LessOrEqual(ISqlValue a, ISqlValue b) {
			return Compare(a, b) <= 0;
		}

		public override bool IsComparable(SqlType type) {
			return type is SqlNumericType;
		}

		private static int GetIntSize(SqlTypeCode sqlType) {
			switch (sqlType) {
				case SqlTypeCode.TinyInt:
					return 1;
				case SqlTypeCode.SmallInt:
					return 2;
				case SqlTypeCode.Integer:
					return 4;
				case SqlTypeCode.BigInt:
					return 8;
				default:
					return 0;
			}
		}


		private static int GetFloatSize(SqlTypeCode sqlType) {
			switch (sqlType) {
				default:
					return 0;
				case SqlTypeCode.Real:
					return 4;
				case SqlTypeCode.Float:
				case SqlTypeCode.Double:
					return 8;
			}
		}

		public override SqlType Wider(SqlType otherType) {
			var t1SqlType = TypeCode;
			var t2SqlType = otherType.TypeCode;
			if (t1SqlType == SqlTypeCode.Decimal) {
				return this;
			}
			if (t2SqlType == SqlTypeCode.Decimal) {
				return otherType;
			}
			if (t1SqlType == SqlTypeCode.Numeric) {
				return this;
			}
			if (t2SqlType == SqlTypeCode.Numeric) {
				return otherType;
			}

			if (t1SqlType == SqlTypeCode.Bit) {
				return otherType; // It can't be any smaller than a Bit
			}
			if (t2SqlType == SqlTypeCode.Bit) {
				return this;
			}

			int t1IntSize = GetIntSize(t1SqlType);
			int t2IntSize = GetIntSize(t2SqlType);
			if (t1IntSize > 0 && t2IntSize > 0) {
				// Both are int types, use the largest size
				return (t1IntSize > t2IntSize) ? this : otherType;
			}

			int t1FloatSize = GetFloatSize(t1SqlType);
			int t2FloatSize = GetFloatSize(t2SqlType);
			if (t1FloatSize > 0 && t2FloatSize > 0) {
				// Both are floating types, use the largest size
				return (t1FloatSize > t2FloatSize) ? this : otherType;
			}

			if (t1FloatSize > t2IntSize) {
				return this;
			}
			if (t2FloatSize > t1IntSize) {
				return otherType;
			}
			if (t1IntSize >= t2FloatSize || t2IntSize >= t1FloatSize) {
				// Must be a long (8 bytes) and a real (4 bytes), widen to a double
				return new SqlNumericType(SqlTypeCode.Double, 16, -1);
			}

			// NOTREACHED - can't get here, the last three if statements cover
			// all possibilities.
			throw new InvalidOperationException("Widest type error.");
		}

		public override bool CanCastTo(ISqlValue value, SqlType destType) {
			if (!(value is SqlNumber))
				return false;

			var number = (SqlNumber) value;
			if (destType is SqlCharacterType) {
				var charType = (SqlCharacterType) destType;
				return !charType.HasMaxSize ||
				       charType.MaxSize >= number.Precision;
			}

			// TODO: pre-check if the binary type can hold the binary version
			return destType is SqlBinaryType ||
			       destType is SqlNumericType;
		}

		public override ISqlValue Cast(ISqlValue value, SqlType destType) {
			if (!(value is SqlNumber))
				throw new ArgumentException();

			var number = (SqlNumber) value;

			if (destType is SqlBinaryType)
				return ToBinary(number, (SqlBinaryType) destType);
			if (destType is SqlCharacterType)
				return ToString(number, (SqlCharacterType) destType);
			if (destType is SqlNumericType)
				return ToNumeric(number, (SqlNumericType) destType);

			return base.Cast(value, destType);
		}

		private ISqlValue ToNumeric(SqlNumber number, SqlNumericType destType) {
			// TODO: should we make some checks here?
			return destType.NormalizeValue(number);
		}

		private ISqlValue ToBinary(SqlNumber number, SqlBinaryType destType) {
			var bytes = number.ToByteArray();

			if (bytes.Length > destType.MaxSize)
				return SqlNull.Value;

			return destType.NormalizeValue(new SqlBinary(bytes));
		}

		private ISqlValue ToString(SqlNumber number, SqlCharacterType destType) {
			if (destType.HasMaxSize && number.Precision > destType.MaxSize)
				return SqlNull.Value;

			var s = number.ToString();
			return destType.NormalizeValue(new SqlString(s));
		}

		public override ISqlValue NormalizeValue(ISqlValue value) {
			if (value is SqlNull)
				return value;

			if (!(value is SqlNumber))
				throw new ArgumentException();

			var number = (SqlNumber) value;

			switch (TypeCode) {
				case SqlTypeCode.TinyInt:
				case SqlTypeCode.SmallInt:
				case SqlTypeCode.Integer:
				case SqlTypeCode.BigInt:
					return ToInteger(number);
				case SqlTypeCode.Real:
				case SqlTypeCode.Float:
				case SqlTypeCode.Double:
					return ToFloatingPoint(number);
				case SqlTypeCode.Numeric:
					return ToDecimal(number);
			}

			return base.NormalizeValue(value);
		}

		private SqlNumber ToDecimal(SqlNumber number) {
			if (SqlNumber.IsNaN(number))
				return SqlNumber.NaN;
			if (SqlNumber.IsNegativeInfinity(number))
				return SqlNumber.NegativeInfinity;
			if (SqlNumber.IsPositiveInfinity(number))
				return SqlNumber.PositiveInfinity;

			var thisDiff = Precision - Scale;
			var otherDiff = number.Precision - Scale;
			if (thisDiff == otherDiff)
				return number;

			var value = number.innerValue;

			if (thisDiff > otherDiff) {
				value = BigMath.Scale(value, Scale);
			} else {
				value = BigMath.Scale(value, Scale-thisDiff);
			}

			return new SqlNumber(SqlNumber.NumericState.None, value);
		}

		private SqlNumber ToInteger(SqlNumber number) {
			switch (TypeCode) {
				case SqlTypeCode.TinyInt:
					return (SqlNumber)(byte) number;
				case SqlTypeCode.SmallInt:
					return (SqlNumber)(short) number;
				case SqlTypeCode.Integer:
					return (SqlNumber)(int) number;
				case SqlTypeCode.BigInt:
					return (SqlNumber) (long) number;
				default:
					throw new InvalidCastException();
			}
		}

		private SqlNumber ToFloatingPoint(SqlNumber number) {
			switch (TypeCode) {
				case SqlTypeCode.Float:
				case SqlTypeCode.Real:
					return (SqlNumber) (float) number;
				case SqlTypeCode.Double:
					return (SqlNumber) (double) number;
				default:
					throw new InvalidCastException();
			}
		}

		public override ISqlValue UnaryPlus(ISqlValue value) {
			if (!(value is SqlNumber))
				return SqlNull.Value;

			return +(SqlNumber) value;
		}

		public override ISqlValue Not(ISqlValue value) {
			if (!(value is SqlNumber))
				return SqlNull.Value;

			return ~(SqlNumber) value;
		}

		public override ISqlValue Negate(ISqlValue value) {
			if (!(value is SqlNumber))
				return SqlNull.Value;

			return -(SqlNumber) value;
		}

		public override ISqlValue Add(ISqlValue a, ISqlValue b) {
			if (!(a is SqlNumber) ||
				!(b is SqlNumber))
				return SqlNull.Value;

			var x = (SqlNumber) a;
			var y = (SqlNumber) b;

			return SqlMath.Add(x, y, Precision);
		}

		public override ISqlValue Subtract(ISqlValue a, ISqlValue b) {
			if (!(a is SqlNumber) ||
			    !(b is SqlNumber))
				return SqlNull.Value;

			var x = (SqlNumber)a;
			var y = (SqlNumber)b;

			return SqlMath.Subtract(x, y, Precision);
		}

		public override ISqlValue Multiply(ISqlValue a, ISqlValue b) {
			if (!(a is SqlNumber) ||
			    !(b is SqlNumber))
				return SqlNull.Value;

			var x = (SqlNumber)a;
			var y = (SqlNumber)b;

			return SqlMath.Multiply(x, y);
		}

		public override ISqlValue Divide(ISqlValue a, ISqlValue b) {
			if (!(a is SqlNumber) ||
			    !(b is SqlNumber))
				return SqlNull.Value;

			var x = (SqlNumber)a;
			var y = (SqlNumber)b;

			return SqlMath.Divide(x, y, Precision);
		}

		public override ISqlValue Modulo(ISqlValue a, ISqlValue b) {
			if (!(a is SqlNumber) ||
			    !(b is SqlNumber))
				return SqlNull.Value;

			var x = (SqlNumber)a;
			var y = (SqlNumber)b;

			return SqlMath.Remainder(x, y);
		}

		public override ISqlValue XOr(ISqlValue a, ISqlValue b) {
			if (!(a is SqlNumber) ||
			    !(b is SqlNumber))
				return SqlNull.Value;

			var x = (SqlNumber)a;
			var y = (SqlNumber)b;

			return x ^ y;
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			base.AppendTo(builder);

			if (TypeCode == SqlTypeCode.Numeric) {
				if (Precision > 0) {
					builder.Append("(");
					builder.Append(Precision);
					if (Scale > 0) {
						builder.Append(",");
						builder.Append(Scale);
					}
					builder.Append(")");
				}
			}
		}
	}
}