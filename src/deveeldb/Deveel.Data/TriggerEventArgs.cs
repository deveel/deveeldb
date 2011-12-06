// 
//  Copyright 2011  Deveel
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
	public delegate void TriggerEventHandler(object sender, TriggerEventArgs args);

	public sealed class TriggerEventArgs : EventArgs {
		private readonly string triggerName;

		/// <summary>
		/// The type of this event.
		/// </summary>
		private readonly TriggerEventType type;

		/// <summary>
		/// The source of the trigger (eg. the table name).
		/// </summary>
		private readonly string source;

		/// <summary>
		/// The number of times this event was fired.
		/// </summary>
		private readonly int count;

		internal TriggerEventArgs(TriggerEventType type, String source, int count) 
			: this(null, type, source, count) {
		}

		internal TriggerEventArgs(string triggerName, TriggerEventType type, string source, int count) {
			this.triggerName = triggerName;
			this.type = type;
			this.source = source;
			this.count = count;
		}

		public string TriggerName {
			get { return triggerName; }
		}

		/// <summary>
		/// Returns the type of this event.
		/// </summary>
		public TriggerEventType Type {
			get { return type; }
		}

		///<summary>
		/// Returns the source of this event.
		///</summary>
		public string Source {
			get { return source; }
		}

		/// <summary>
		/// Returns the number of times this event was fired.
		/// </summary>
		public int Count {
			get { return count; }
		}

		public bool IsInsert {
			get { return (type & TriggerEventType.Insert) != 0; }
		}

		public bool IsUpdate {
			get { return (type & TriggerEventType.Update) != 0; }
		}

		public bool IsDelete {
			get { return (type & TriggerEventType.Delete) != 0; }
		}

		public bool IsBefore {
			get { return (type & TriggerEventType.Before) != 0; }
		}

		public bool IsAfter {
			get { return (type & TriggerEventType.After) != 0; }
		}
	}
}