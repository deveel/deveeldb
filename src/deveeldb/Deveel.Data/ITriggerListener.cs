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

namespace Deveel.Data {
	///<summary>
	/// A listener that can listen for high layer trigger events.
	///</summary>
	public interface ITriggerListener {
		///<summary>
		/// Notifies that a trigger event fired.
		///</summary>
		///<param name="database">The <see cref="DatabaseConnection"/> that this 
		/// trigger is registered for.</param>
		///<param name="trigger_name">The name of the trigger fired.</param>
		///<param name="trigger_evt">The trigger event that was fired.</param>
		void FireTrigger(DatabaseConnection database, String trigger_name, TriggerEvent trigger_evt);
	}
}