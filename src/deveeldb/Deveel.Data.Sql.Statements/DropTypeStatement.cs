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

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropTypeStatement : SqlStatement {
		public DropTypeStatement(ObjectName typeName) {
			if (typeName == null)
				throw new ArgumentNullException("typeName");

			TypeName = typeName;
		}

		private DropTypeStatement(SerializationInfo info, StreamingContext context) {
			TypeName = (ObjectName) info.GetValue("TypeName", typeof(ObjectName));
		}

		public ObjectName TypeName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TypeName", TypeName);
		}
	}
}