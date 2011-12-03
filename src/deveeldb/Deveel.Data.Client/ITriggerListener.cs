// 
//  Copyright 2010  Deveel
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

namespace Deveel.Data.Client {
	/// <summary>
	/// A listener that is notified when the trigger being listened to is fired.
	/// </summary>
	public interface ITriggerListener {
		/// <summary>
		/// Notifies this listener that the trigger with the name has been fired.
		/// </summary>
		/// <param name="e">The argument object that describes the information
		/// about a fired trigger.</param>
		/// <remarks>
		/// Trigger's are specified via the SQL syntax and a trigger listener can 
		/// be registered via <see cref="DeveelDbConnection"/>.
		/// </remarks>
		/// <seealso cref="TriggerEventArgs"/>
		void OnTriggerFired(TriggerEventArgs e);
	}
}