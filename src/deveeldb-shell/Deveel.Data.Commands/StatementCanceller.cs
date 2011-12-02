using System;
using System.Runtime.CompilerServices;
using System.Threading;

using Deveel.Shell;

namespace Deveel.Data.Commands {
	/// <summary>
	/// A thread to be used to cancel a statement running in 
	/// another thread.
	/// </summary>
	sealed class StatementCanceller : IInterruptable {
		private readonly Thread thread;
		private readonly CancelTarget _target;
		private bool _armed;
		private bool _running;
		private volatile bool _cancelStatement;


		/// <summary>
		/// The target to be cancelled.
		/// </summary>
		/// <remarks>
		/// Must not throw an Execption and may to whatever it 
		/// needs to do.
		/// </remarks>
		public interface CancelTarget {
			void CancelRunningStatement();
		}

		public StatementCanceller(CancelTarget target) {
			thread = new Thread(Run);
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
		public void StartThread() {
			thread.Start();
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void StopThread() {
			_running = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Arm() {
			_armed = true;
			_cancelStatement = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Disarm() {
			_armed = false;
			_cancelStatement = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		private void Run() {
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
							_target.CancelRunningStatement();
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