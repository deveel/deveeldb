// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.DbSystem {
	public class ObjectNotFoundException : DatabaseSystemException {
		public ObjectNotFoundException(ObjectName objectName)
			: this(objectName, String.Format("The object name '{0}' does not reference any object in the system.", objectName)) {
		}

		public ObjectNotFoundException(string message)
			: this(null, message) {
		}

		public ObjectNotFoundException(ObjectName objectName, string message)
			: base(SystemErrorCodes.ObjectNotFound, message) {
			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }
	}
}