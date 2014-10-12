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

namespace Deveel.Data {
	/// <summary>
	/// A concrete implementation of <see cref="IStringAccessor"/> 
	/// that uses a <see cref="string"/> object.
	/// </summary>
	[Serializable]
	public class StringObject : IStringAccessor {

		/// <summary>
		/// The <see cref="string"/> object.
		/// </summary>
		private readonly String str;

		private StringObject(String str) {
			this.str = str;
		}

		/// <summary>
		/// Returns the length of the string.
		/// </summary>
		public int Length {
			get { return str.Length; }
		}

		/// <summary>
		/// Returns a <see cref="TextReader"/> that can read 
		/// from the string.
		/// </summary>
		/// <returns></returns>
		public TextReader GetTextReader() {
			return new StringReader(str);
		}

		public override bool Equals(object obj) {
			if (obj == null)
				return false;
			if (obj is string)
				return str == (obj as string);
			if (obj is StringObject) {
				StringObject sobj = (StringObject) obj;
				if (str == null && sobj.str == null)
					return true;
				if (str == null && sobj.str != null)
					return false;
				return Equals(str, sobj.str);
			}

			throw new ArgumentException("Cannot compare to a string.");
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public override String ToString() {
			return str;
		}

		/// <summary>
		/// Static method that returns a <see cref="StringObject"/> from the 
		/// given <see cref="string"/>.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static StringObject FromString(String str) {
			return str != null ? new StringObject(str) : null;
		}
	}
}