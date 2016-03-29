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
using System.Collections.Generic;

using Deveel.Data.Services;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// Represents the origin of system events, providing
	/// a mechanism to fill the metadata before dispatching
	/// the event.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Implementations of this interface provide the event, despite
	/// of where it was generated, with data that will be carried on
	/// with the event to the final destination.
	/// </para>
	/// <para>
	/// A system event can be handled by many sources, and this will
	/// provide the final system or user a more informational data
	/// that can trigger second or third level actions.
	/// </para>
	/// </remarks>
	public interface IEventSource {
		IContext Context { get; }

		/// <summary>
		/// Gets an optional parent source.
		/// </summary>
		/// <value>
		/// The optional parent source.
		/// </value>
		IEventSource ParentSource { get; }

		/// <summary>
		/// Gets the list of metadata associated to the source.
		/// </summary>
		/// <value>
		/// The list of the source metadata.
		/// </value>
		IEnumerable<KeyValuePair<string, object>> Metadata { get; } 
	}
}
