// 
//  Copyright 2010-2018 Deveel
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
using System.Collections.Generic;

using Deveel.Data.Events;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public class ObjectCreatedEvent : Event {
		public ObjectCreatedEvent(IEventSource source, DbObjectType objectType, ObjectName objectName) : base(source) {
			if (ObjectName.IsNullOrEmpty(objectName))
				throw new ArgumentNullException(nameof(objectName));

			ObjectType = objectType;
			ObjectName = objectName;
		}

		public DbObjectType ObjectType { get; }

		public ObjectName ObjectName { get; }

		protected override void GetEventData(IDictionary<string, object> data) {
			data["obj.type"] = ObjectType.ToString().ToLowerInvariant();
			data["obj.name"] = ObjectName.ToString();
		}
	}
}