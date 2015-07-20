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

namespace Deveel.Data.Sql.Triggers {
	/// <summary>
	/// An object that defines the arguments of an event, used 
	/// to find triggers associated.
	/// </summary>
	public sealed class TriggerEventInfo {
		/// <summary>
		/// Constructs the <see cref="TriggerEventInfo"/> object for the
		/// given table name and event type.
		/// </summary>
		/// <param name="tableName">The fully qualified name of the table.</param>
		/// <param name="eventType">The type of event that happened on the table.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="tableName"/> is <c>null</c>.
		/// </exception>
		public TriggerEventInfo(ObjectName tableName, TriggerEventType eventType) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			EventType = eventType;
		}

		/// <summary>
		/// Gets the fully qualified name of the table where the event happened.
		/// </summary>
		public ObjectName TableName { get; private set; }

		/// <summary>
		/// Gets the type of event that happened on the table.
		/// </summary>
		public TriggerEventType EventType { get; private set; }
	}
}
