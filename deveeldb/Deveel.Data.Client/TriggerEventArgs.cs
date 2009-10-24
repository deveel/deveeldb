//  
//  TriggerEventArgs.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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