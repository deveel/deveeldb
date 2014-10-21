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
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// This is an event occurred during the lifetime of a database.
	/// </summary>
	public interface IDatabaseEvent {
		/// <summary>
		/// Gets a value that identifies the type of event
		/// </summary>
		/// <seealso cref="EventType"/>
		byte EventType { get; }

		/// <summary>
		/// Gets the class code of the event
		/// </summary>
		int EventClass { get; }

		/// <summary>
		/// Gets a unique event code within the class of event.
		/// </summary>
		int EventCode { get; }

		/// <summary>
		/// Gets a message that describes the message that occurred.
		/// </summary>
		string EventMessage { get; }

		/// <summary>
		/// Gets additional event data that come with the event.
		/// </summary>
		IDictionary<string, object> EventData { get; }
	}
}