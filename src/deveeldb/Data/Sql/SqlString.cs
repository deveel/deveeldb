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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The most simple implementation of a SQL string with a small size
	/// </summary>
	/// <remarks>
	/// <para>
	/// Instances of this object handle strings that are not backed by large
	/// objects and can handle a fixed length of characters.
	/// </para>
	/// <para>
	/// The encoding of the string is dependent from the <see cref="SqlCharacterType"/> that
	/// defines an object, but the default is <see cref="UnicodeEncoding"/>.
	/// </para>
	/// </remarks>
	/// <seealso cref="ISqlString"/>
	[DebuggerDisplay("{ToString()}")]
	public sealed class SqlString : ISqlString, IEquatable<SqlString>, IConvertible {
		/// <summary>
		/// The maximum length of characters a <see cref="SqlString"/> can handle.
		/// </summary>
		public const int MaxLength = Int16.MaxValue;

		private readonly string source;

		/// <summary>
		/// Initializes a new instance of the <see cref="SqlString"/> structure with
		/// the given set of characters.
		/// </summary>
		/// <param name="chars">The chars.</param>
		public SqlString(char[] chars)
			: this(chars, chars?.Length ?? 0) {
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SqlString"/> structure.
		/// </summary>
		/// <param name="chars">The chars.</param>
		/// <param name="length">The length.</param>
		/// <exception cref="System.ArgumentOutOfRangeException">length</exception>
		public SqlString(char[] chars, int length) {
			if (chars == null) {
				source = null;
			} else {
				if (length > MaxLength)
					throw new ArgumentOutOfRangeException(nameof(length));

				source = new string(chars, 0, length);
				Length = chars.Length;
			}
		}

		public SqlString(string source)
			: this(source == null ? (char[]) null : source.ToCharArray()) {
		}

		public SqlString(byte[] bytes, int offset, int length)
			: this(GetChars(bytes, offset, length)) {
		}

		public SqlString(byte[] bytes)
			: this(bytes, 0, bytes == null ? 0 : bytes.Length) {
		}

		private static char[] GetChars(byte[] bytes, int offset, int length) {
			if (bytes == null)
				return null;

			return Encoding.Unicode.GetChars(bytes, offset, length);
		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((ISqlString) obj);
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			return CompareTo((ISqlString) other);
		}

		public string Value => source;

	    public char this[long index] {
			get {
				if (index > Int32.MaxValue)
					throw new ArgumentOutOfRangeException(nameof(index));

				if (source == null)
					return '\0';
				if (index >= Length)
					throw new ArgumentOutOfRangeException(nameof(index));

				return source[(int) index];
			}
		}

        public static readonly SqlString Empty = new SqlString(String.Empty);

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return other is ISqlString;
		}

		public int CompareTo(ISqlString other) {
			if (other == null)
				throw new ArgumentNullException(nameof(other));

			if (other is SqlString) {
				var otherString = (SqlString) other;
				return String.Compare(Value, otherString.Value, StringComparison.Ordinal);
			}

			throw new NotImplementedException("Comparison with long strong not implemented yet.");
		}

		public IEnumerator<char> GetEnumerator() {
			return new StringEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public int Length { get; }

		long ISqlString.Length => Length;

		public TextReader GetInput() {
			return new StringReader(source);
		}

		public SqlString Substring(int offset, int count) {
			if (offset < 0)
				throw new ArgumentOutOfRangeException(nameof(offset));
			if (count >= Length)
				throw new ArgumentOutOfRangeException(nameof(count));
			if (offset + count > Length)
				throw new ArgumentOutOfRangeException();

			return new SqlString(source.Substring(offset, count));
		}

		public SqlString PadRight(int length)
			=> PadRight(length, ' ');

		public SqlString PadRight(int length, char c) {
			if (length < 0)
				throw new ArgumentException();

			return new SqlString(source.PadRight(length, c));
		}

		public SqlString PadLeft(int length, char c) {
			if (length < 0)
				throw new ArgumentException();

			return new SqlString(source.PadLeft(length, c));
		}

		public SqlString PadLeft(int length)
			=> PadLeft(length, ' ');

		public bool Equals(SqlString other) {
			return Equals(other, false);
		}

		public bool Equals(SqlString other, bool ignoreCase) {
			if (source == null && other.source == null)
				return true;
			if (source == null)
				return false;

			if (source.Length != other.source.Length)
				return false;

			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return source.Equals(other.source, comparison);
		}

		public override bool Equals(object obj) {
			if (!(obj is SqlString))
				return false;

			return Equals((SqlString) obj);
		}

		public override int GetHashCode() {
			if (source == null)
				return 0;

			unchecked {
				int hash = 17;

				// get hash code for all items in array
				foreach (var item in source) {
					hash = hash * 23 + item.GetHashCode();
				}

				return hash;
			}
		}

		public byte[] ToByteArray() {
			if (source == null)
				return new byte[0];

			return Encoding.Unicode.GetBytes(source);
		}

		public SqlString Concat(ISqlString other) {
			if (other == null)
				return this;

			if (other is SqlString) {
				var otheString = (SqlString) other;
				var length = (int) (Length + otheString.Length);
				if (length >= MaxLength)
					throw new ArgumentException("The final string will be over the maximum length");

				var sourceChars = ToCharArray();
				var otherChars = otheString.ToCharArray();
				var destChars = new char[length];

				Array.Copy(sourceChars, 0, destChars, 0, (int) Length);
				Array.Copy(otherChars, 0, destChars, (int) Length, (int) otheString.Length);
				return new SqlString(destChars, length);
			}

			var sb = new StringBuilder(Int16.MaxValue);
			using (var output = new StringWriter(sb)) {
				// First read the current stream
				using (var reader = GetInput()) {
					var buffer = new char[2048];
					int count;
					while ((count = reader.Read(buffer, 0, buffer.Length)) != 0) {
						output.Write(buffer, 0, count);
					}
				}

				// Then read the second stream
				using (var reader = other.GetInput()) {
					var buffer = new char[2048];
					int count;
					while ((count = reader.Read(buffer, 0, buffer.Length)) != 0) {
						output.Write(buffer, 0, count);
					}
				}

				output.Flush();
			}

			var outChars = new char[sb.Length];
			sb.CopyTo(0, outChars, 0, sb.Length);
			return new SqlString(outChars, outChars.Length);
		}

		#region StringEnumerator

		class StringEnumerator : IEnumerator<char> {
			private readonly SqlString sqlString;
			private int index = -1;
			private readonly int length;

			public StringEnumerator(SqlString sqlString) {
				this.sqlString = sqlString;
				length = (int) sqlString.Length;
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				return ++index < length;
			}

			public void Reset() {
				index = -1;
			}

			public char Current {
				get {
					if (index >= length)
						throw new InvalidOperationException();

					return sqlString[index];
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion

		public override string ToString() {
			return Value;
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.String;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return Convert.ToBoolean(Value, provider);
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			return Convert.ToChar(Value, provider);
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			return Convert.ToSByte(Value, provider);
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return Convert.ToByte(Value, provider);
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return Convert.ToInt16(Value, provider);
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			return Convert.ToUInt16(Value, provider);
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return Convert.ToInt32(Value, provider);
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			return Convert.ToUInt32(Value, provider);
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return Convert.ToInt64(Value, provider);
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			return Convert.ToUInt64(Value, provider);
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return Convert.ToSingle(Value, provider);
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return Convert.ToDouble(Value, provider);
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			return Convert.ToDecimal(Value, provider);
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			return Convert.ToDateTime(Value, provider);
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return Convert.ToString(Value, provider);
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(char[]))
				return ToCharArray();
			if (conversionType == typeof(DateTimeOffset))
				return ToDateTimeOffset(provider);

			if (conversionType == typeof(SqlNumber))
				return ToNumber(provider);
			if (conversionType == typeof(SqlBoolean))
				return ToBoolean();
			if (conversionType == typeof(SqlDateTime))
				return ToSqlDateTime(provider);
			if (conversionType == typeof(SqlBinary))
				return ToBinary();

			throw new InvalidCastException(String.Format("Cannot convet SQL STRING to {0}", conversionType.FullName));
		}

		private DateTimeOffset ToDateTimeOffset(IFormatProvider provider) {
			return DateTimeOffset.Parse(Value, provider);
		}

		private SqlBoolean ToBoolean() {
			SqlBoolean value;
			if (!SqlBoolean.TryParse(Value, out value))
				throw new FormatException();

			return value;
		}

		private SqlNumber ToNumber(IFormatProvider provider) {
			SqlNumber value;
			if (!SqlNumber.TryParse(Value, provider, out value))
				throw new FormatException();

			return value;
		}

		public SqlDateTime ToSqlDateTime(IFormatProvider provider) {
			SqlDateTime value;
			if (!SqlDateTime.TryParse(Value, out value))
				throw new FormatException();

			return value;
		}

		private SqlBinary ToBinary() {
			var bytes = ToByteArray();
			return new SqlBinary(bytes);
		}

		public char[] ToCharArray() {
			if (source == null)
				return new char[0];

			return source.ToCharArray();
		}

		#region Operators

		public static bool operator ==(SqlString s1, SqlString s2) {
			if (ReferenceEquals(s1, null) &&
			    ReferenceEquals(s2, null))
				return true;
			if (ReferenceEquals(s1, null))
				return false;
			if (ReferenceEquals(s2, null))
				return false;

			return s1.Equals(s2);
		}

		public static bool operator !=(SqlString s1, SqlString s2) {
			return !(s1 == s2);
		}

		#endregion

		public static explicit operator SqlString(string s) {
			return new SqlString(s);
		}
	}
}