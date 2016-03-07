using System;

namespace System.Runtime.Serialization {
	[Serializable]
	public struct StreamingContext {
		public StreamingContext(StreamingContextStates state) 
			: this(state, null) {
		}

		public StreamingContext(StreamingContextStates state, object context) {
			State = state;
			Context = context;
		}

		public StreamingContextStates State { get; private set; }

		public object Context { get; private set; }

		public override bool Equals(object obj) {
			if (!(obj is StreamingContext))
				return false;

			var other = (StreamingContext) obj;
			return State == other.State &&
			       Context == other.Context;
		}

		public override int GetHashCode() {
			return (int)State;
		}
	}
}
