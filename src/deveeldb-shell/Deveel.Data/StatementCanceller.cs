//
//  Copyright 2011 Deveel
//
//  This file is part of DeveelDBShell.
//
//  DeveelDBShell is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  DeveelDBShell is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with DeveelDBShell. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Runtime.CompilerServices;
using System.Threading;

using Deveel.Console;

namespace Deveel.Data {
	/// <summary>
	/// A thread to be used to cancel a statement running in 
	/// another thread.
	/// </summary>
	sealed class StatementCanceller : IInterruptable {
		private readonly Thread thread;
		private readonly CancelTarget target;
		private bool armed;
		private bool running;
		private volatile bool cancelStatement;


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
			cancelStatement = false;
			armed = false;
			running = true;
			this.target = target;
		}

		/** inherited: interruptable interface */
		public void Interrupt() {
			cancelStatement = true;
			/* watch out, we must not call notify, since we
			 * are in the midst of a signal handler */
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void StartThread() {
			thread.Start();
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void StopThread() {
			running = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Arm() {
			armed = true;
			cancelStatement = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Disarm() {
			armed = false;
			cancelStatement = false;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		private void Run() {
			try {
				while(true) {
					while (running && !armed) {
						Monitor.Wait(this);
					}
					if (!running)
						return;

					while (armed && !cancelStatement) {
						Monitor.Wait(this, 300);
					}
					if (cancelStatement) {
						try {
							target.CancelRunningStatement();
						} catch (Exception) {
							/* ignore */
						}

						armed = false;
					}
				}
			} catch (ThreadInterruptedException e) {
				return;
			}
		}
	}
}