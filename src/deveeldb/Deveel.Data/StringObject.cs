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
using System.Globalization;
using System.IO;
using System.Text;

using Deveel.Data.Types;

namespace Deveel.Data {
	[Serializable]
	public sealed class StringObject : DataObject, IEquatable<StringObject>, IComparable<StringObject>,
		IStringAccessor {
		private readonly char[] source;

		public StringObject(StringType type, char[] chars, int length)
			: base(type) {
			if (chars == null) {
				source = null;
			} else {
				source = new char[length];
				Array.Copy(chars, 0, source, 0, length);
				Length = length;
			}
		}

		public StringObject(StringType type, char[] chars)
			: this(type, chars, chars == null ? 0 : chars.Length) {
		}

		public StringObject(StringType type, byte[] bytes)
			: this(type, type.Encoding.GetChars(bytes)) {
		}

		public StringObject(StringType type, string source)
			: this(type, type.Encoding.GetBytes(source)) {
		}

		public int Length { get; private set; }

		public override bool IsNull {
			get { return source == null; }
		}

		public override bool Equals(object obj) {
			var other = obj as StringObject;
			if (other == null)
				return false;

			return Equals(other);
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

		public bool Equals(StringObject other) {
			if (other == null)
				return false;

			if (!Type.Equals(other.Type))
				return false;

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

		public int CompareTo(StringObject other) {
			return Type.Compare(this, other);
		}

		public TextReader GetTextReader() {
			var chars = source;
			if (chars == null)
				chars = new char[0];

			return new StringReader(new string(chars));
		}

		public override string ToString() {
			using (var reader = GetTextReader()) {
				return reader.ReadToEnd();
			}
		}

		public StringObject Concat(StringObject other) {
			// If this or val is null then return the null value
			if (IsNull)
				return this;
			if (other.IsNull)
				return other;

			var otherType = other.Type;

			if (otherType is StringType) {
				// Pick the first locale,
				var st1 = (StringType)Type;
				var st2 = (StringType)otherType;

				CultureInfo locale = null;

				if (st1.Locale != null) {
					locale = st1.Locale;
				} else if (st2.Locale != null) {
					locale = st2.Locale;
				}

				var destType = st1;
				if (locale != null)
					destType = PrimitiveTypes.String(st1.SqlType, st1.MaxSize, locale);

				var sb = new StringBuilder(Int16.MaxValue);
				using (var output = new StringWriter(sb)) {
					// First read the current stream
					using (var reader = GetTextReader()) {
						var buffer = new char[2048];
						int count;
						while ((count = reader.Read(buffer, 0, buffer.Length)) != 0) {
							output.Write(buffer, 0, count);
						}
					}

					// Then read the second stream
					using (var reader = other.GetTextReader()) {
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
				return new StringObject(destType, outChars);
			}

			return Null((StringType)Type);
		}

		public static StringObject Null(StringType stringType) {
			return new StringObject(stringType, (char[])null);
		}
	}
}