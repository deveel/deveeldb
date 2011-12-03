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
	public sealed class TriggerEventArgs : EventArgs {
		private readonly string source;
		private readonly string triggerName;
		private readonly TriggerEventType triggerType;
		private readonly int fireCount;

		internal TriggerEventArgs(string source, string triggerName, TriggerEventType triggerType, int fireCount) {
			this.source = source;
			this.fireCount = fireCount;
			this.triggerType = triggerType;
			this.triggerName = triggerName;
		}

		public int FireCount {
			get { return fireCount; }
		}

		public TriggerEventType TriggerType {
			get { return triggerType; }
		}

		public bool IsInsert {
			get { return (triggerType & TriggerEventType.Insert) != 0; }
		}

		public bool IsUpdate {
			get { return (triggerType & TriggerEventType.Update) != 0; }
		}

		public bool IsDelete {
			get { return (triggerType & TriggerEventType.Delete) != 0; }
		}

		public bool IsBefore {
			get { return (triggerType & TriggerEventType.Before) != 0; }
		}

		public bool IsAfter {
			get { return (triggerType & TriggerEventType.After) != 0; }
		}

		public string TriggerName {
			get { return triggerName; }
		}

		public string Source {
			get { return source; }
		}
	}
}