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
	/// <summary>
	/// A default implementation of <see cref="DatabaseEventHandler"/> which
	/// takes a <see cref="EventHandler"/> as argument to create a generic 
	/// event to dispatch within a database context.
	/// </summary>
	/// <remarks>
	/// In this implementation the <see cref="Execute"/> method calls
	/// the <see cref="EventHandler.Invoke"/> method with a <b>null</b>
	/// sender and an <see cref="EventArgs.Empty">empty</see> argument.
	/// </remarks>
	internal class DatabaseEventHandler : IDatabaseEvent {
		public DatabaseEventHandler(EventHandler handler) {
			this.handler = handler;
		}

		private readonly EventHandler handler;

		public void Execute() {
			handler(null, EventArgs.Empty);
		}
	}
}