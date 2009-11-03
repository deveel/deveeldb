using System;
using System.Runtime.CompilerServices;
using System.Threading;

using Deveel.Data.Util;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	/**
 * After arming, this runnable will display the current time
 * after some timeout. Used in long running SQL-statements
 * to show a) how long it took so far b) keep terminal
 * sessions open that are otherwise being closed by some
 * firewalls :-)
 *
 * @author hzeller
 * @version $Revision: 1.1 $
 */
	public class LongRunningTimeDisplay {
		private readonly long _startTimeDisplayAfter;
		private readonly String _message;
		private readonly CancelWriter _timeDisplay;
		private DateTime _lastArmTime;
		private volatile bool _running;
		private volatile bool _armed;

		public LongRunningTimeDisplay(String msg, long showAfter) {
			_startTimeDisplayAfter = showAfter;
			_message = msg;
			_running = true;
			_armed = false;
			_timeDisplay = new CancelWriter(OutputDevice.Message);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void arm() {
			_lastArmTime = DateTime.Now;
			_armed = true;
			Monitor.Pulse(this);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void disarm() {
			if (_armed) {
				_armed = false;
				_timeDisplay.cancel();
				Monitor.Pulse(this);
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void stopThread() {
			_running = false;
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
					DateTime startDisplayAt = _lastArmTime.AddMilliseconds(_startTimeDisplayAfter);
					while (_armed && DateTime.Now < startDisplayAt) {
						Monitor.Wait(this, 300);
					}
					while (_armed) {
						long totalTime = (long)DateTime.Now.Subtract(_lastArmTime).TotalMilliseconds;
						totalTime -= totalTime % 1000; // full seconds.
						String time = TimeRenderer.RenderTime(totalTime);
						_timeDisplay.cancel();
						_timeDisplay.print(_message + " " + time);
						Monitor.Wait(this, 5000);
					}
					_timeDisplay.cancel();
				}
			} catch (ThreadInterruptedException e) {
				return;
			}
		}
	}
}