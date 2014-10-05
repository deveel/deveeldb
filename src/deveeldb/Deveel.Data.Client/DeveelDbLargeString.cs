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

namespace Deveel.Data.Client {
	public class DeveelDbLargeString : DeveelDbLargeObject {
		public DeveelDbLargeString(ReferenceType referenceType, long length)
			: this(referenceType, length, FileAccess.ReadWrite) {
		}

		public DeveelDbLargeString(ReferenceType referenceType, long length, FileAccess access)
			: base(referenceType, length, access) {
			if (referenceType != ReferenceType.AsciiText &&
				referenceType != ReferenceType.UnicodeText)
				throw new ArgumentException("Not a valid reference type: must be ASCII or UNICODE text.");
		}

		public override string ToString() {
			// TODO: Read the entire string from the channel ...
			return base.ToString();
		}
	}
}