// 
//  Copyright 2010-2016 Deveel
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
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Objects {
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
	[Serializable]
	public struct SqlBoolean : ISqlObject, IEquatable<SqlBoolean>, IComparable<SqlBoolean>, ISerializable
#if !PCL
		, IConvertible
#endif
		{
		private readonly byte? value;

		/// <summary>
		/// Represents the materialization of a <c>true</c> boolean.
		/// </summary>
		public static readonly SqlBoolean True = new SqlBoolean(1);

		/// <summary>
		/// Represents the materialization of a <c>false</c> boolean.
		/// </summary>
		public static readonly SqlBoolean False = new SqlBoolean(0);

		/// <summary>
		/// Defines a <c>null</c> boolean.
		/// </summary>
		public static readonly SqlBoolean Null = new SqlBoolean((byte?)null);

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
			: this((byte)(value ? 1 : 0)) {
		}

		private SqlBoolean(byte? value)
			: this() {
			this.value = value;
		}

		private SqlBoolean(SerializationInfo info, StreamingContext context)
			: this() {
			value = (byte?) info.GetValue("Value", typeof (byte?));
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is ISqlObject))
				throw new ArgumentException();

			return CompareTo((ISqlObject) obj);
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Value", value, typeof(byte?));
		}

		/// <inheritdoc/>
		public int CompareTo(ISqlObject other) {
			if (other is SqlNull) {
				if (IsNull)
					return 0;
				return 1;
			}

			SqlBoolean otherBoolean;
			if (other is SqlNumber) {
				var num = (SqlNumber) other;
				if (num.IsNull) {
					otherBoolean = Null;
				} else if (num == SqlNumber.One) {
					otherBoolean = True;
				} else if (num == SqlNumber.Zero) {
					otherBoolean = False;
				} else {
					throw new ArgumentOutOfRangeException("other", "The given numeric value is out of range for a comparison with SQL BOOLEAN.");
				}
			} else if (other is SqlBoolean) {
				otherBoolean = (SqlBoolean) other;
			} else {
				throw new ArgumentException(String.Format("Object of type {0} cannot be compared to SQL BOOLEAN", other.GetType().FullName));
			}

			return CompareTo(otherBoolean);
		}

		/// <inheritdoc/>
		public bool IsNull {
			get { return value == null; }
		}

		/// <summary>
		/// Indicates if the given <see cref="ISqlObject"/> can be compared to this
		/// <see cref="SqlBoolean"/>.
		/// </summary>
		/// <param name="other">The other object to verifiy compatibility.</param>
		/// <remarks>
		/// Comparable objects are <see cref="SqlBoolean"/>, <see cref="SqlNull"/> or
		/// <see cref="SqlNumber"/> with a value equals to <see cref="SqlNumber.One"/>,
		/// <see cref="SqlNumber.Zero"/> or <see cref="SqlNumber.Null"/>.
		/// </remarks>
		/// <returns>
		/// Returns <c>true</c> if the given object can be compared to this boolean
		/// instance, or <c>false</c> otherwise.
		/// </returns>
		public bool IsComparableTo(ISqlObject other) {
			if (other is SqlBoolean || other is SqlNull)
				return true;

			if (other is SqlNumber) {
				var num = (SqlNumber) other;
				return num == SqlNumber.Zero || num == SqlNumber.One;
			}

			return false;
		}

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			if (obj is SqlNull && IsNull)
				return true;

			if (!(obj is SqlBoolean))
				return false;

			return Equals((SqlBoolean) obj);
		}

		/// <inheritdoc/>
		public bool Equals(SqlBoolean other) {
			if (IsNull && other.IsNull)
				return true;
			if (IsNull && !other.IsNull)
				return false;
			if (!IsNull && other.IsNull)
				return false;

			return value.Equals(other.value);
		}

		public SqlBoolean Not() {
			if (value == null)
				return Null;

			if (value == 1)
				return False;
			if (value == 0)
				return True;

			throw new InvalidOperationException();
		}

		public SqlBoolean Or(SqlBoolean other) {
			if (value == null ||
				other.IsNull)
				return Null;

			if (value == 1 || other.value == 1)
				return True;

			return False;
		}

		public SqlBoolean And(SqlBoolean other) {
			if (value == null ||
				other.IsNull)
				return Null;

			if (value == 1 && other.value == 1)
				return True;

			return False;			
		}

		public SqlBoolean XOr(SqlBoolean other) {
			if (value == null ||
			    other.IsNull)
				return Null;

			if (value == 1 && other.value == 0)
				return True;
			if (value == 0 && other.value == 1)
				return True;

			return False;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return value == null ? 0 : value.Value.GetHashCode();
		}

#if !PCL
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
			if (value == null)
				throw new NullReferenceException();

			return value.Value;
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return (short) (this as IConvertible).ToInt32(provider);
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			return (ushort) (this as IConvertible).ToInt32(provider);
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			if (value == null)
				throw new NullReferenceException();

			return value.Value;
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			return (uint) (this as IConvertible).ToInt32(provider);
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return (this as IConvertible).ToInt64(provider);
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			return (this as IConvertible).ToUInt64(provider);
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
			if (conversionType == typeof (bool))
				return (bool) this;

			throw new InvalidCastException(String.Format("Cannot convert a SQL BOOLEAN to {0}", conversionType.FullName));
		}

#endif

		/// <inheritdoc/>
		public int CompareTo(SqlBoolean other) {
			if (other.IsNull && IsNull)
				return 0;

			if (IsNull && !other.IsNull)
				return -1;
			if (!IsNull && other.IsNull)
				return 1;

			return value.Value.CompareTo(other.value.Value);
		}

		/// <summary>
		/// Operates an equality check between the two SQL boolean objects.
		/// </summary>
		/// <param name="a">The left term of comparison.</param>
		/// <param name="b">The right term of comparison.</param>
		/// <returns></returns>
		/// <seealso cref="Equals(SqlBoolean)"/>
		/// <seealso cref="Equals(object)"/>
		public static SqlBoolean operator ==(SqlBoolean a, SqlBoolean b) {
			return a.Equals(b);
		}

		/// <summary>
		/// Operates an inequality check between the two SQL booleans
		/// </summary>
		/// <param name="a">The left term of comparison.</param>
		/// <param name="b">The right term of comparison.</param>
		/// <returns></returns>
		public static SqlBoolean operator !=(SqlBoolean a, SqlBoolean b) {
			return !(a == b);
		}

		public static SqlBoolean operator ==(SqlBoolean a, ISqlObject b) {
			return a.Equals(b);
		}

		public static SqlBoolean operator !=(SqlBoolean a, ISqlObject b) {
			return !(a == b);
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
			return a.Not();
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
			if (value.IsNull)
				throw new InvalidCastException();

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
			if (value == null)
				return "NULL";
			if (value == 1)
				return "true";
			if (value == 0)
				return "false";

			throw new InvalidOperationException("Should never happen!");
		}
	}
}