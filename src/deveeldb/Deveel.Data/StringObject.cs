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
using System.Runtime.Remoting.Messaging;

using Deveel.Data.Types;

namespace Deveel.Data {
	public sealed class StringObject : DataObject, IEquatable<StringObject>, IComparable, IComparable<StringObject>,
		IStringAccessor {
		private readonly char[] source;

		public StringObject(StringType type, char[] chars, int length)
			: base(type) {
		}

		public override bool Equals(object obj) {
			return base.Equals(obj);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public bool Equals(StringObject other) {
			throw new NotImplementedException();
		}

		int IComparable.CompareTo(object obj) {
			
		}

		public int CompareTo(StringObject other) {
			throw new NotImplementedException();
		}

		public int Length { get; private set; }

		public TextReader GetTextReader() {
			throw new NotImplementedException();
		}
	}
}