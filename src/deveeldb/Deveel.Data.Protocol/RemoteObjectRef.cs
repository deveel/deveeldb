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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Store;

namespace Deveel.Data.Deveel.Data.Protocol {
	public sealed class RemoteObjectRef : IObjectRef, ISqlObject {
		public RemoteObjectRef(ObjectId objectId, long size) {
			ObjectId = objectId;
			Size = size;
		}

		public ObjectId ObjectId { get; private set; }

		public long Size { get; private set; }

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		bool ISqlObject.IsNull {
			get { return false; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}
	}
}
