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

using Deveel.Data.Types;

namespace Deveel.Data {
	[Serializable]
	public abstract class DataObject {
		protected DataObject(DataType type) {
			if (type == null)
				throw new ArgumentNullException("type");

			Type = type;
		}

		public DataType Type { get; private set; }

		public virtual bool IsNull {
			get { return false; }
		}

		public bool IsComparable(DataObject obj) {
			return Type.IsComparable(obj.Type);
		}

		public int SizeOf() {
			return Type.SizeOf(this);
		}
	}
}