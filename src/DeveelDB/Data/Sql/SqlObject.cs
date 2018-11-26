// 
//  Copyright 2010-2018 Deveel
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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class SqlObject : IComparable<SqlObject>, IComparable, ISqlFormattable, IEquatable<SqlObject> {
		public static readonly SqlObject Unknown = new SqlObject(PrimitiveTypes.Boolean(), null);
		public static readonly SqlObject Null = new SqlObject(PrimitiveTypes.Integer(), null);
		public static readonly SqlObject True = new SqlObject(PrimitiveTypes.Boolean(), new SqlBoolean(true));
		public static readonly SqlObject False = new SqlObject(PrimitiveTypes.Boolean(), new SqlBoolean(false));

		public SqlObject(SqlType type, ISqlValue value) {
			if (type == null)
				throw new ArgumentNullException(nameof(type));

			if (value == null)
				value = SqlNull.Value;

			if (!type.IsInstanceOf(value))
				throw new ArgumentException($"The value given is not an instance of {type}", nameof(value));

			Type = type;
			Value = type.NormalizeValue(value);
		}

		public ISqlValue Value { get; }

		public SqlType Type { get; }

		public bool IsNull => !(Type is SqlBooleanType) && SqlNull.Value == Value;

		public bool IsUnknown => Type is SqlBooleanType && SqlNull.Value == Value;

		public bool IsTrue => Type is SqlBooleanType && (SqlBoolean) Value == SqlBoolean.True;

		public bool IsFalse => Type is SqlBooleanType && (SqlBoolean) Value == SqlBoolean.False;

		private int CompareToNotNull(SqlObject other) {
			var type = Type;

			// Strings must be handled as a special case.
			if (type is SqlCharacterType) {
				// We must determine the locale to compare against and use that.
				var stype = (SqlCharacterType) type;

				// If there is no locale defined for this type we use the locale in the
				// given type.
				if (stype.Locale == null) {
					type = other.Type;
				}
			}

			return type.Compare(Value, other.Value);

		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is SqlObject))
				throw new ArgumentException();

			return CompareTo((SqlObject) obj);
		}

		public int CompareTo(SqlObject obj) {
			if (IsUnknown && obj.IsUnknown)
				return 0;
			if (IsUnknown && !obj.IsUnknown)
				return -1;
			if (!IsUnknown && obj.IsUnknown)
				return 1;

			if (IsNull && obj.IsNull)
				return 0;
			if (IsNull && !obj.IsNull)
				return -1;
			if (!IsNull && obj.IsNull)
				return 1;

			// otherwise both are non null so compare normally.
			return CompareToNotNull(obj);
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (IsUnknown) {
				builder.Append("UNKNOWN");
			}
			else if (IsNull) {
				builder.Append("NULL");
			}
			else {
				builder.Append(Type.ToSqlString(Value));
			}
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		public override int GetHashCode() {
			unchecked {
				var code = Type.GetHashCode() * 23;
				code = code ^ Value.GetHashCode();

				return code;
			}
		}

		public override bool Equals(object obj) {
			if (!(obj is SqlObject))
				return false;

			var other = (SqlObject) obj;

			return Equals(other);
		}

		public bool Equals(SqlObject other) {
			if (ReferenceEquals(other, null))
				return false;

			if (!Type.Equals(other.Type))
				return false;

			if (SqlNull.Value == Value &&
			    SqlNull.Value == other.Value)
				return true;
			if (SqlNull.Value == Value ||
			    SqlNull.Value == other.Value)
				return false;

			return Value.Equals(other.Value);
		}

		private SqlObject BinaryOperator(Func<SqlType, Func<ISqlValue, ISqlValue, ISqlValue>> selector,
			SqlObject other) {
			if (IsNull || (other == null || other.IsNull))
				return Null;
			if (IsUnknown || other.IsUnknown)
				return Unknown;

			if (!Type.IsComparable(other.Type))
				throw new ArgumentException($"Type {Type} is not comparable to type {other.Type} of the argument");

			// TODO: should instead return null?

			var resultType = Type.Wider(other.Type);
			var op = selector(resultType);
			var result = op(Value, other.Value);

			if (SqlNull.Value != result &&
			    !resultType.IsInstanceOf(result))
				resultType = GetSqlType(result);

			return new SqlObject(resultType, result);
		}

		private SqlObject RelationalOperator(Func<SqlType, Func<ISqlValue, ISqlValue, SqlBoolean>> selector,
			SqlObject other) {
			if (IsNull || (other == null || other.IsNull))
				return Unknown;
			if (IsUnknown || other.IsUnknown)
				return Unknown;

			if (!Type.IsComparable(other.Type))
				throw new ArgumentException($"Type {Type} is not comparable to type {other.Type} of the argument");

			// TODO: should instead return null?

			var op = selector(Type);
			var result = op(Value, other.Value);

			return new SqlObject(PrimitiveTypes.Boolean(), result);
		}

		#region Relational Operators

		public SqlObject Equal(SqlObject other) {
			return RelationalOperator(type => type.Equal, other);
		}

		public SqlObject NotEqual(SqlObject other) {
			return RelationalOperator(type => type.NotEqual, other);
		}

		public SqlObject GreaterThan(SqlObject other) {
			return RelationalOperator(type => type.Greater, other);
		}

		public SqlObject GreaterThanOrEqual(SqlObject other) {
			return RelationalOperator(type => type.GreaterOrEqual, other);
		}

		public SqlObject LessThan(SqlObject other) {
			return RelationalOperator(type => type.Less, other);
		}

		public SqlObject LessOrEqualThan(SqlObject other) {
			return RelationalOperator(type => type.LessOrEqual, other);
		}

		public SqlObject Is(SqlObject other) {
			if (IsUnknown && other.IsUnknown)
				return New(SqlBoolean.True);
			if (IsUnknown && !other.IsUnknown ||
			    !IsUnknown && other.IsUnknown)
				return New(SqlBoolean.False);

			if (Type is SqlBooleanType &&
			    other.Type is SqlBooleanType) {
				var b1 = (SqlBoolean) Value;
				var b2 = (SqlBoolean) other.Value;

				return New((SqlBoolean) (b1 == b2));
			}

			return New(SqlBoolean.False);
		}

		public SqlObject IsNot(SqlObject other) {
			return Is(other).Not();
		}

		#endregion

		#region Logical Operators

		// Note: AND and OR can be logical but also bitwise operators

		public SqlObject Or(SqlObject other) {
			if (IsUnknown && other.IsUnknown)
				return Unknown;
			if (IsUnknown && other.IsTrue)
				return other;
			if (IsTrue && other.IsUnknown)
				return this;

			return BinaryOperator(type => type.Or, other);
		}

		public SqlObject XOr(SqlObject other) {
			if (IsUnknown && other.IsUnknown)
				return Unknown;
			if (IsUnknown && other.IsTrue)
				return other;
			if (IsTrue && other.IsUnknown)
				return this;

			return BinaryOperator(type => type.XOr, other);
		}

		public SqlObject And(SqlObject other) {
			if (IsUnknown && other.IsUnknown)
				return Unknown;
			if (IsUnknown && other.IsFalse)
				return other;
			if (IsFalse && other.IsUnknown)
				return this;

			return BinaryOperator(type => type.And, other);
		}

		#endregion

		#region Binary Operators

		public SqlObject Add(SqlObject other) {
			return BinaryOperator(type => type.Add, other);
		}

		public SqlObject Subtract(SqlObject other) {
			return BinaryOperator(type => type.Subtract, other);
		}

		public SqlObject Multiply(SqlObject other) {
			return BinaryOperator(type => type.Multiply, other);
		}

		public SqlObject Divide(SqlObject other) {
			return BinaryOperator(type => type.Divide, other);
		}

		public SqlObject Modulo(SqlObject other) {
			return BinaryOperator(type => type.Modulo, other);
		}

		#endregion

		#region Unary Operators

		private SqlObject UnaryOperator(Func<SqlType, Func<ISqlValue, ISqlValue>> selector) {
			if (IsNull || IsUnknown)
				return this;

			var resultType = Type;
			var op = selector(resultType);
			var result = op(Value);

			if (SqlNull.Value != result &&
			    !resultType.IsInstanceOf(result))
				resultType = GetSqlType(result);

			return new SqlObject(resultType, result);
		}

		public SqlObject Not() {
			return UnaryOperator(type => type.Negate);
		}

		public SqlObject Negate() {
			return UnaryOperator(type => type.Negate);
		}

		public SqlObject Plus() {
			return UnaryOperator(type => type.UnaryPlus);
		}

		#endregion

		#region Cast Operator

		public bool CanCastTo(SqlType destType) {
			return Type.CanCastTo(Value, destType);
		}

		public SqlObject CastTo(SqlType destType) {
			if (IsUnknown)
				return this;

			if (!CanCastTo(destType))
				return new SqlObject(destType, null);

			var result = Type.Cast(Value, destType);

			return new SqlObject(destType, result);
		}

		#endregion

		#region Factories

		private static SqlType GetSqlType(ISqlValue value) {
			if (value == null ||
			    SqlNull.Value == value)
				throw new ArgumentException();

			if (value is SqlNumber) {
				var number = (SqlNumber) value;

				if (number.CanBeInt32)
					return PrimitiveTypes.Integer();
				if (number.CanBeInt64)
					return PrimitiveTypes.BigInt();

				if (number.Precision == SqlNumericType.FloatPrecision)
					return new SqlNumericType(SqlTypeCode.Float, number.Precision, number.Scale);
				if (number.Precision == SqlNumericType.DoublePrecision)
					return new SqlNumericType(SqlTypeCode.Double, number.Precision, number.Scale);
				if (number.Precision == SqlNumericType.DecimalPrecision)
					return new SqlNumericType(SqlTypeCode.Decimal, number.Precision, number.Scale);

				return PrimitiveTypes.Numeric(number.Precision, number.Scale);
			}

			if (value is ISqlString) {
				// TODO: support the long string
				var length = ((ISqlString) value).Length;

				return PrimitiveTypes.VarChar((int) length);
			}

			if (value is SqlBinary) {
				var bin = (SqlBinary) value;

				return PrimitiveTypes.VarBinary((int) bin.Length);
			}

			if (value is SqlDateTime) {
				return PrimitiveTypes.TimeStamp();
			}

			if (value is SqlBoolean)
				return PrimitiveTypes.Boolean();

			if (value is SqlYearToMonth)
				return PrimitiveTypes.YearToMonth();
			if (value is SqlDayToSecond)
				return PrimitiveTypes.DayToSecond();

			if (value is SqlArray)
				return PrimitiveTypes.Array(((SqlArray) value).Length);

			throw new NotSupportedException();
		}

		public static SqlObject New(ISqlValue value) {
			return new SqlObject(GetSqlType(value), value);
		}

		public static SqlObject NullOf(SqlType type) {
			return new SqlObject(type, SqlNull.Value);
		}

		#region Boolean Objects

		public static SqlObject Boolean(SqlBoolean? value) {
			return new SqlObject(PrimitiveTypes.Boolean(), value);
		}

		public static SqlObject Bit(SqlBoolean? value)
			=> new SqlObject(PrimitiveTypes.Bit(), value);

		#endregion

		#region String Objects

		public static SqlObject String(SqlString value) {
			return new SqlObject(PrimitiveTypes.String(), value);
		}

		public static SqlObject String(string value)
			=> String(new SqlString(value));

		#endregion

		#region Numeric

		public static SqlObject Integer(int value) {
			return new SqlObject(PrimitiveTypes.Integer(), (SqlNumber) value);
		}

		public static SqlObject BigInt(long value) {
			return new SqlObject(PrimitiveTypes.BigInt(), (SqlNumber) value);
		}

		public static SqlObject Double(double value) {
			return new SqlObject(PrimitiveTypes.Double(), (SqlNumber) value);
		}

		public static SqlObject Numeric(SqlNumber value) {
			return new SqlObject(PrimitiveTypes.Numeric(value.Precision, value.Scale), value);
		}

		#endregion

		#region Array

		public static SqlObject Array(SqlArray array) {
			return new SqlObject(PrimitiveTypes.Array(array.Length), array);
		}

		public static SqlObject Array(params SqlObject[] items) {
			var array = items == null
				? new SqlArray(new SqlExpression[0])
				: new SqlArray(items.Select(SqlExpression.Constant).Cast<SqlExpression>().ToArray());

			return Array(array);
		}

		public static SqlObject Array(params SqlExpression[] expressions) {
			var array = expressions == null ? new SqlArray(new SqlExpression[0]) : new SqlArray(expressions);

			return Array(array);
		}

		#endregion

		#endregion
	}
}