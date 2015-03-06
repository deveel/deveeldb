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
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public sealed class ErrorEvent : IDatabaseEvent {
		internal ErrorEvent(int eventClass, int errorCode, string message, IDictionary<string, object> data) {
			EventClass = eventClass;
			ErrorCode = errorCode;
			Message = message;
			Data = data;
		}

		public int ErrorCode { get; private set; }

		public string Message { get; private set; }

		public IDictionary<string, object> Data { get; private set; }

		byte IDatabaseEvent.EventType {
			get { return (byte) EventType.Error; }
		}

		public int EventClass { get; private set; }

		int IDatabaseEvent.EventCode {
			get { return ErrorCode; }
		}

		string IDatabaseEvent.EventMessage {
			get { return Message; }
		}

		IDictionary<string, object> IDatabaseEvent.EventData {
			get { return Data; }
		}
	}
}