using System;
using System.Collections.Generic;

namespace Deveel.Data.Client {
	public sealed class InterruptHandler {
		private readonly Stack<IInterruptable> stack;
		private bool interrupted;

		private InterruptHandler() {
			stack = new Stack<IInterruptable>();
			interrupted = false;
		}

		static InterruptHandler() {
			Default = new InterruptHandler();
		}

		public static InterruptHandler Default { get; private set; }

		public void Push<T>(T obj) where T : IInterruptable {
			stack.Push(obj);
		}

		public void Pop() {
			stack.Pop();
			interrupted = false;
		}

		public void Reset() {
			interrupted = false;
			stack.Clear();
		}

		public void Signal() {
			if (interrupted)
				return;

			try {
				if (stack.Count > 0) {
					foreach (var interruptable in stack) {
						interruptable.Interrupt();
						stack.Pop();
					}
				} else {
					Output.Current.WriteLine("[Ctrl-C ; interrupted]: nothing to interrupt");
				}
			} finally {
				interrupted = true;
			}
		}
	}
}
