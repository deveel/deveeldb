using System;
using System.Runtime.CompilerServices;
using System.Threading;

using Deveel.Shell;

namespace Deveel.Data.Commands {
	/**
 * A thread to be used to cancel a statement running
 * in another thread.
 */
	sealed class StatementCanceller : IInterruptable {
		private readonly CancelTarget _target;
		private bool _armed;
		private bool _running;
		private volatile bool _cancelStatement;

		/**
		 * The target to be cancelled. Must not throw an Execption
		 * and may to whatever it needs to do.
		 */
		public interface CancelTarget {
			void cancelRunningStatement();
		}

		public StatementCanceller(CancelTarget target) {
			_cancelStatement = false;
			_armed = false;
			_running = true;
			_target = target;
		}

		/** inherited: interruptable interface */
		public void Interrupt() {
			_cancelStatement = true;
			/* watch out, we must not call notify, since we
			 * are in the midst of a signal handler */
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void stopThread() {
			_running = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void arm() {
			_armed = true;
			_cancelStatement = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void disarm() {
			_armed = false;
			_cancelStatement = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void run() {
			try {
				for (; ; ) {
					while (_running && !_armed) {
						Monitor.Wait(this);
					}
					if (!_running) return;
					while (_armed && !_cancelStatement) {
						Monitor.Wait(this, 300);
					}
					if (_cancelStatement) {
						try {
							_target.cancelRunningStatement();
						} catch (Exception e) {
							/* ignore */
						}
						_armed = false;
					}
				}
			} catch (ThreadInterruptedException e) {
				return;
			}
		}
	}
}