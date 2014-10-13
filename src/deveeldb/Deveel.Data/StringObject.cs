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
using System.IO;

using Deveel.Data.Types;

namespace Deveel.Data {
	[Serializable]
	public sealed class StringObject : DataObject, IEquatable<StringObject>, IComparable, IComparable<StringObject>,
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

		int IComparable.CompareTo(object obj) {
			if (!(obj is StringObject))
				throw new ArgumentException();

			var other = obj as StringObject;
			return CompareTo(other);
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
	}
}