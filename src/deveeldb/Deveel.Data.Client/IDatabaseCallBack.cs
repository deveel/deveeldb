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
	///<summary>
	/// An interface that is input to the IDatabaseInterface as a way to be
	/// notified of event information from inside the database.
	///</summary>
	public interface IDatabaseCallBack {
		///<summary>
		/// Called when the database has generated an event that this user is 
		/// listening for.
		///</summary>
		///<param name="event_type"></param>
		///<param name="event_message"></param>
		/// <remarks>
		/// The thread that calls back these events is always a volatile thread 
		/// that may not block.  It is especially important that no queries are 
		/// executed when this calls back.  To safely act on events, it is advisable 
		/// to dispatch onto another thread.
		/// </remarks>
		void OnDatabaseEvent(int event_type, String event_message);
	}
}