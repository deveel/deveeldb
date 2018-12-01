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

namespace Deveel.Data.Events {
	/// <summary>
	/// This is an event occurred during the lifetime of a system
	/// or a database.
	/// </summary>
	public interface IEvent {
		/// <summary>
		/// Gets the event source.
		/// </summary>
		/// <value>
		/// The event source.
		/// </value>
		IEventSource EventSource { get; }

		/// <summary>
		/// Gets the timestamp when the event was first fired
		/// </summary>
		DateTimeOffset TimeStamp { get; }

		/// <summary>
		/// Gets a globally unique identifier of the event
		/// </summary>
		Guid EventId { get; }

		/// <summary>
		/// Gets additional event data that come with the event.
		/// </summary>
		/// <remarks>
		/// This is a dynamic set of key/value data pairs that
		/// can be filled by event sources and consumed by specialized
		/// handlers.
		/// </remarks>
		IDictionary<string, object> EventData { get; }
	}
}