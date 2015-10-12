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

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerCallbackEvent : IEvent {
		IEventSource IEvent.EventSource {
			get {
				// TODO:
				return null;
			}
		}

		byte IEvent.EventType {
			get { return (byte) EventType.Trigger; }
		}

		int IEvent.EventClass {
			get { return EventClasses.SqlModel; }
		}

		int IEvent.EventCode {
			get {
				// TODO:
				return -1;
			}
		}

		string IEvent.EventMessage {
			get { throw new NotImplementedException(); }
		}

		IDictionary<string, object> IEvent.EventData {
			get { throw new NotImplementedException(); }
		}
	}
}
