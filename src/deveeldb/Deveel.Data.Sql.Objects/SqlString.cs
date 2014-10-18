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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public struct SqlString : ISqlString, IEquatable<SqlString>, IConvertible {
		public const int MaxLength = Int16.MaxValue;

		public static readonly SqlString Null = new SqlString((char[])null);
		public static readonly SqlString Empty = new SqlString(new char[0]);

		private readonly char[] source;

		private SqlString(int codePage, char[] chars)
			: this(codePage, chars, chars == null ? 0 : chars.Length) {
		}

		private SqlString(int codePage, char[] chars, int length)
			: this() {
			CodePage = codePage;

			if (chars == null) {
				source = null;
			} else {
				if (length > MaxLength)
					throw new ArgumentOutOfRangeException("length");

				source = new char[length];
				Array.Copy(chars, 0, source, 0, length);
			}			
		}

		public SqlString(char[] chars)
			: this(Encoding.Unicode.CodePage, chars, chars == null ? 0 : chars.Length) {
		}

		public SqlString(string s)
			: this(s == null ? null : s.ToCharArray()) {
		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((ISqlString) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			return CompareTo((ISqlString) other);
		}

		public bool IsNull {
			get { return source == null; }
		}

		public string Value {
			get { return source == null ? null : new string(source); }
		}

		public char this[int index] {
			get {
				if (source == null)
					return '\0';
				if (index >= Length)
					throw new ArgumentOutOfRangeException("index");

				return source[index];
			}
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return other is ISqlString;
		}

		public int CompareTo(ISqlString other) {
			throw new NotImplementedException();
		}

		public IEnumerator<char> GetEnumerator() {
			return new StringEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long Length {
			get { return source == null ? 0 : source.LongLength; }
		}

		public int CodePage { get; private set; }

		public TextReader GetInput() {
			var s = String.Empty;
			if (source != null)
				s = new string(source);

			return new StringReader(s);
		}

		public bool Equals(SqlString other) {
			if (source == null && other.source == null)
				return true;

			if (source == null)
				return false;

			if (source.Length != other.source.Length)
				return false;

			for (int i = 0; i < source.Length; i++) {
				var c1 = source[i];
				var c2 = other.source[i];
				if (!c1.Equals(c2))
					return false;
			}

			return true;
		}

		public override bool Equals(object obj) {
			return Equals((SqlString) obj);
		}

		public override int GetHashCode() {
			if (source == null)
				return 0;

			unchecked {
				int hash = 17;

				// get hash code for all items in array
				foreach (var item in source) {
					hash = hash*23 + item.GetHashCode();
				}

				return hash;
			}
		}

		public static SqlString Unicode(byte[] bytes) {
			return new SqlString(Encoding.Unicode.CodePage, Encoding.Unicode.GetChars(bytes));
		}

		public static SqlString BigEndianUnicode(byte[] bytes) {
			return new SqlString(Encoding.BigEndianUnicode.CodePage, Encoding.BigEndianUnicode.GetChars(bytes));
		}

		public static SqlString Ascii(byte[] bytes) {
			return new SqlString(Encoding.ASCII.CodePage, Encoding.ASCII.GetChars(bytes));
		}

		public static SqlString Decode(int codePage, byte[] bytes) {
			var encoding = Encoding.GetEncoding(codePage);
			return new SqlString(codePage, encoding.GetChars(bytes));
		}

		public byte[] ToByteArray() {
			if (source == null)
				return new byte[0];

			return Encoding.GetEncoding(CodePage).GetBytes(source);
		}

		public SqlString Concat(ISqlString other) {
			if (other == null || other.IsNull)
				return this;

			if (other.CodePage != CodePage)
				throw new ArgumentException("The other string belongs to a different encoding.");

			if (other is SqlString) {
				var otheString = (SqlString) other;
				var length = (int) (Length + otheString.Length);
				if (length >= MaxLength)
					throw new ArgumentException("The final string will be over the maximum length");

				var destChars = new char[length];
				Array.Copy(source, 0, destChars, 0, Length);
				Array.Copy(otheString.source, 0, destChars, Length - 1, otheString.Length);
				return new SqlString(CodePage, destChars, length);
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
			return new SqlString(CodePage, outChars, outChars.Length);
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

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.String;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			throw new NotImplementedException();
		}

		public static bool operator ==(SqlString a, SqlString b) {
			return a.Equals(b);
		}

		public static bool operator !=(SqlString a, SqlString b) {
			return !(a == b);
		}

		public static SqlString operator +(SqlString a, SqlString b) {
			return a.Concat(b);
		}
	}
}