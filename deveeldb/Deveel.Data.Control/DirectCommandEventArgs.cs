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
using System.Runtime.Serialization;

namespace Deveel.Data.Control {
	public sealed class DirectCommandEventArgs : EventArgs {
		internal DirectCommandEventArgs(DirectCommandType commandType, object[] args)
			: this(commandType, args, null) {
		}

		internal DirectCommandEventArgs(DirectCommandType commandType, object[] args, Exception error)
			: this(commandType, args, (object) null) {
			this.error = error;
		}

		internal DirectCommandEventArgs(DirectCommandType commandType, object[] args, object result) {
			this.commandType = commandType;
			this.args = args;
			this.result = result;
		}

		private readonly DirectCommandType commandType;
		private readonly object[] args;
		private readonly object result;
		private readonly Exception error;

		public Exception Error {
			get { return error; }
		}

		public object Result {
			get { return result; }
		}

		public object[] Arguments {
			get { return args; }
		}

		public DirectCommandType CommandType {
			get { return commandType; }
		}
	}
}