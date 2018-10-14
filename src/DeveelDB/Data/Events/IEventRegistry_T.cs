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

namespace Deveel.Data.Events {
	/// <summary>
	/// Implementations of this interface handle the registration
	/// of events fired within the system.
	/// </summary>
	/// <remarks>
	/// <para>
	/// It is delegated to the implementation of the registry to
	/// handle the first instance of the event passed: this can be
	/// the storage of given types of event data, or routing other
	/// events.
	/// </para>
	/// </remarks>
	public interface IEventRegistry<TEvent> : IEventRegistry where TEvent : class, IEvent {
		/// <summary>
		/// Adds the specified event object to the registry.
		/// </summary>
		/// <param name="event">The event object to be registered.</param>
		void Register(TEvent @event);
	}
}
