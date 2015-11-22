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

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// Defines the basic logic for the dispatching of events
	/// within a system workflow.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Event routers take an input event object and are configured
	/// to either transform it or forward it to other components
	/// in the system's life-cycle.
	/// </para>
	/// <para>
	/// Implementations of this interface are typically middleware
	/// that involve in the life-cycle third party components that
	/// listen to system events and react accordingly.
	/// </para>
	/// </remarks>
	public interface IEventRouter {
		bool CanRoute(IEvent @event);

		/// <summary>
		/// Routes the input event to the final destination.
		/// </summary>
		/// <param name="e">The system event to be routed.</param>
		void RouteEvent(IEvent e);
	}
}
