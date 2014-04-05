// 
//  Copyright 2014  Deveel
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
using System.Data;

namespace Deveel.Data.Client {
	public delegate void ConnectionStateEventHandler(object sender, ConnectionStateEventArgs args);

	public sealed class ConnectionStateEventArgs : EventArgs {
		internal ConnectionStateEventArgs(ConnectionState oldState, ConnectionState newState) 
			: this(oldState, newState, null) {
		}

		internal ConnectionStateEventArgs(ConnectionState oldState, Exception error) 
			: this(oldState, ConnectionState.Broken, error) {
		}

		internal ConnectionStateEventArgs(ConnectionState oldState, ConnectionState newState, Exception error) {
			Error = error;
			NewState = newState;
			OldState = oldState;
		}

		public ConnectionState OldState { get; private set; }

		public ConnectionState NewState { get; private set; }

		public Exception Error { get; private set; }

		public bool HasError {
			get { return Error != null; }
		}
	}
}
