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
using System.Diagnostics;

namespace Deveel.Data.Sql {
	/// <summary>
	/// An SQL object handling a single-byte value that represents
	/// the concept of boolean <c>true</c> and <c>false</c>.
	/// </summary>
	/// <remarks>
	/// On a byte level, this object handles only <c>0</c> or <c>1</c>, that
	/// represents respectively the concepts of <c>false</c> and <c>true</c>.
	/// <para>
	/// Additionally, a boolean object can be represented as <c>NULL</c>, when the
	/// state cannot be determined.
	/// </para>
	/// </remarks>
	[DebuggerDisplay("{ToString()}")]
	public struct SqlBoolean : ISqlValue, IEquatable<SqlBoolean>, IComparable<SqlBoolean>, IConvertible {
		private readonly byte value;

		/// <summary>
		/// Represents the materialization of a <c>true</c> boolean.
		/// </summary>
		public static readonly SqlBoolean True = new SqlBoolean(1);

		public static readonly SqlString TrueString = new SqlString("TRUE");

		/// <summary>
		/// Represents the materialization of a <c>false</c> boolean.
		/// </summary>
		public static readonly SqlBoolean False = new SqlBoolean(0);

		public static readonly SqlString FalseString = new SqlString("FALSE");

		/// <summary>
		/// Constructs a given boolean object with a defined byte value.
		/// </summary>
		/// <param name="value">The single byte representing the boolean.</param>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the <paramref name="value"/> specified is not equivalent to
		/// <c>0</c> or <c>1</c>.
		/// </exception>
		public SqlBoolean(byte value)
			: this() {
			if (value != 0 &&
			    value != 1)
				throw new ArgumentOutOfRangeException("value");

			this.value = value;
		}

		/// <summary>
		/// Constructs an object from a runtime boolean object.
		/// </summary>
		/// <param name="value">The native boolean value that represents the object.</param>
		public SqlBoolean(bool value)
			: this((byte) (value ? 1 : 0)) {
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is ISqlValue))
				throw new ArgumentException();

			return CompareTo((ISqlValue) obj);
		}

		/// <inheritdoc/>
		public int CompareTo(ISqlValue other) {
			SqlBoolean otherBoolean;
			if (other is SqlNumber) {
				var num = (SqlNumber) other;
				if (num == SqlNumber.One) {
					otherBoolean = True;
				} else if (num == SqlNumber.Zero) {
					otherBoolean = False;
				} else {
					throw new ArgumentOutOfRangeException("other", "The given numeric value is out of range for a comparison with SQL BOOLEAN.");
				}
			} else if (other is SqlBoolean) {
				otherBoolean = (SqlBoolean) other;
			} else {
				throw new ArgumentException(String.Format("Object of type {0} cannot be compared to SQL BOOLEAN",
					other.GetType().FullName));
			}

			return CompareTo(otherBoolean);
		}

		
		bool ISqlValue.IsComparableTo(ISqlValue other) {
			if (other is SqlBoolean)
				return true;

			if (other is SqlNumber) {
				var num = (SqlNumber) other;
				return num == SqlNumber.Zero ||
				       num == SqlNumber.One;
			}

			return false;
		}

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			if (!(obj is SqlBoolean))
				return false;

			return Equals((SqlBoolean) obj);
		}

		/// <inheritdoc/>
		public bool Equals(SqlBoolean other) {
			return value.Equals(other.value);
		}

		private SqlBoolean Negate() {
			if (value == 1)
				return False;
			if (value == 0)
				return True;

			throw new InvalidOperationException();
		}

		private SqlBoolean Or(SqlBoolean other) {
			if (value == 1 || other.value == 1)
				return True;

			return False;
		}

		private SqlBoolean And(SqlBoolean other) {
			if (value == 1 && other.value == 1)
				return True;

			return False;
		}

		private SqlBoolean XOr(SqlBoolean other) {
			if (value == 1 && other.value == 0)
				return True;
			if (value == 0 && other.value == 1)
				return True;

			return False;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return value.GetHashCode();
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.Boolean;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return this;
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			return (sbyte) (this as IConvertible).ToInt32(provider);
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return value;
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return (short) (this as IConvertible).ToInt32(provider);
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			return (ushort) (this as IConvertible).ToInt32(provider);
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return value;
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			return (uint) (this as IConvertible).ToInt32(provider);
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return (this as IConvertible).ToInt32(provider);
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			return (this as IConvertible).ToUInt32(provider);
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return (this as IConvertible).ToInt32(provider);
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return (this as IConvertible).ToInt32(provider);
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			return (this as IConvertible).ToInt32(provider);
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(SqlNumber))
				return (SqlNumber) value;

			throw new InvalidCastException(String.Format("Cannot convert a SQL BOOLEAN to {0}", conversionType.FullName));
		}

		/// <inheritdoc/>
		public int CompareTo(SqlBoolean other) {
			return value.CompareTo(other.value);
		}

		/// <summary>
		/// Operates an equality check between the two SQL boolean objects.
		/// </summary>
		/// <param name="a">The left term of comparison.</param>
		/// <param name="b">The right term of comparison.</param>
		/// <returns></returns>
		/// <seealso cref="Equals(SqlBoolean)"/>
		/// <seealso cref="Equals(object)"/>
		public static bool operator ==(SqlBoolean a, SqlBoolean b) {
			return a.Equals(b);
		}

		/// <summary>
		/// Operates an inequality check between the two SQL booleans
		/// </summary>
		/// <param name="a">The left term of comparison.</param>
		/// <param name="b">The right term of comparison.</param>
		/// <returns></returns>
		public static bool operator !=(SqlBoolean a, SqlBoolean b) {
			return !a.Equals(b);
		}

		public static SqlBoolean operator >(SqlBoolean a, SqlBoolean b) {
			return a.CompareTo(b) > 0;
		}

		public static SqlBoolean operator <(SqlBoolean a, SqlBoolean b) {
			return a.CompareTo(b) < 0;
		}

		public static SqlBoolean operator <=(SqlBoolean a, SqlBoolean b) {
			return a.CompareTo(b) <= 0;
		}

		public static SqlBoolean operator >=(SqlBoolean a, SqlBoolean b) {
			return a.CompareTo(b) >= 0;
		}

		public static SqlBoolean operator &(SqlBoolean a, SqlBoolean b) {
			return a.And(b);
		}

		public static SqlBoolean operator |(SqlBoolean a, SqlBoolean b) {
			return a.Or(b);
		}

		public static SqlBoolean operator ^(SqlBoolean a, SqlBoolean b) {
			return a.XOr(b);
		}

		public static SqlBoolean operator !(SqlBoolean a) {
			return a.Negate();
		}

		public static bool operator true(SqlBoolean b) {
			return b;
		}

		public static bool operator false(SqlBoolean b) {
			return !b;
		}

		/// <summary>
		/// Implicitly converts the SQL boolean object to a native boolean.
		/// </summary>
		/// <param name="value">The SQL boolean to convert.</param>
		/// <returns>
		/// Returns an instance of <see cref="bool"/> which is equivalent to 
		/// this object.
		/// </returns>
		public static implicit operator bool(SqlBoolean value) {
			return value.value == 1;
		}

		public static implicit operator bool?(SqlBoolean value) {
			return value.value == 1;
		}

		/// <summary>
		/// Implicitly converts a given native boolean to the SQL object equivalent.
		/// </summary>
		/// <param name="value">The boolean value to convert.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlBoolean"/> that is equivalent
		/// to the boolean value given.
		/// </returns>
		public static implicit operator SqlBoolean(bool value) {
			return new SqlBoolean(value);
		}

		/// <summary>
		/// Parses the given string to extract a boolean value equivalent.
		/// </summary>
		/// <param name="s">The string to parse.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlBoolean"/> as defined by the 
		/// given string.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the string argument <paramref name="s"/> is <c>null</c> or empty.
		/// </exception>
		/// <exception cref="FormatException">
		/// If the given string argument cannot be parsed to a valid SQL boolean.
		/// </exception>
		/// <seealso cref="TryParse"/>
		public static SqlBoolean Parse(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			SqlBoolean value;
			if (!TryParse(s, out value))
				throw new FormatException();

			return value;
		}

		/// <summary>
		/// Attempts to parse a given string to an instance of <see cref="SqlBoolean"/>.
		/// </summary>
		/// <param name="s">The string to parse.</param>
		/// <param name="value">The output <see cref="SqlBoolean"/> that will be emitted
		/// if the given string represents a valid boolean.</param>
		/// <remarks>
		/// A valid SQL boolean is either a numeric value, expressed by <c>1</c> or <c>0</c>,
		/// or alternatively a string value expressed as <c>true</c> or <c>false</c>.
		/// <para>
		/// The case of the string is insensitive (as for general SQL syntax rule).
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <c>true</c> if the string passed is a valid SQL boolean and the value
		/// was set to a valid <see cref="SqlBoolean"/>, or <c>false</c> otherwise.
		/// </returns>
		public static bool TryParse(string s, out SqlBoolean value) {
			value = new SqlBoolean();

			if (String.IsNullOrEmpty(s))
				return false;

			if (String.Equals(s, "true", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(s, "1")) {
				value = True;
				return true;
			}
			if (String.Equals(s, "false", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(s, "0")) {
				value = False;
				return true;
			}

			return false;
		}

		/// <inheritdoc/>
		public override string ToString() {
			if (value == 1)
				return "true";
			if (value == 0)
				return "false";

			throw new InvalidOperationException("Should never happen!");
		}
	}
}