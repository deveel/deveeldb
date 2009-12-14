using System;

namespace Deveel.Data.Client {
	public sealed class TriggerEventArgs : EventArgs {
		internal TriggerEventArgs(string source, string triggerName, TriggerEventType triggerType, int fireCount) {
			this.source = source;
			this.fireCount = fireCount;
			this.triggerType = triggerType;
			this.triggerName = triggerName;
		}

		private readonly string source;
		private readonly string triggerName;
		private readonly TriggerEventType triggerType;
		private readonly int fireCount;

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