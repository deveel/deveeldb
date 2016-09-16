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

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class DroppedModifiedObjectConflictException : TransactionException {
		internal DroppedModifiedObjectConflictException(ObjectName objectName)
			: base(SystemErrorCodes.DroppedModifiedObjectConflict, String.Format("Object '{0}' was modified and dropped.", objectName)) {
			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }
	}
}
