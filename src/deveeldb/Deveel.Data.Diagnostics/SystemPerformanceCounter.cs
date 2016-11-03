// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Diagnostics;
using System.Threading;

namespace Deveel.Data.Diagnostics {
	public sealed class SystemPerformanceCounter : ICounter, IDisposable {
		private Timer timer;
		private PerformanceCounter counter;

		public SystemPerformanceCounter(string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Name = name;

			counter = CreateCounter();

			// TODO: make the callback time configurable?
			timer = new Timer(Count, null, 0, 100);
		}

		~SystemPerformanceCounter() {
			Dispose(false);
		}

		public string Name { get; private set; }

		public object Value { get; private set; }

		private void Count(object state) {
			Value = counter.NextValue();
		}

		private PerformanceCounter CreateCounter() {
			var category = GetCounterCategory();
			var counterName = GetCounterName();
			var instance = GetInstanceName();

			return new PerformanceCounter(category, counterName, instance, true);
		}

		private string GetInstanceName() {
			switch (Name) {
				case KnownSystemCounterNames.SystemMemoryUsage:
				case KnownSystemCounterNames.SystemProcessorUsage:
					return Process.GetCurrentProcess().ProcessName;
				// case KnownSystemCounterNames.MachineMemoryUsage:
				case KnownSystemCounterNames.MachineProcessorUsage:
					return "_Total";
				default:
					return null;
			}
		}

		private string GetCounterName() {
			switch (Name) {
				case KnownSystemCounterNames.MachineMemoryUsage:
					return "Available MBytes";
				case KnownSystemCounterNames.SystemMemoryUsage:
					return "Working Set";
				case KnownSystemCounterNames.MachineProcessorUsage:
				case KnownSystemCounterNames.SystemProcessorUsage:
					return "% Processor Time";
				default:
					return null;
			}
		}

		private string GetCounterCategory() {
			switch (Name) {
				case KnownSystemCounterNames.MachineMemoryUsage:
					return "Memory";
				case KnownSystemCounterNames.MachineProcessorUsage:
					return "Processor";
				case KnownSystemCounterNames.SystemProcessorUsage:
				case KnownSystemCounterNames.SystemMemoryUsage:
					return "Process";
				default:
					return null;
			}
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (timer != null)
					timer.Dispose();

				if (counter != null)
					counter.Dispose();
			}

			timer = null;
			counter = null;
		}

		public void Dispose() {
			Dispose(false);
			GC.SuppressFinalize(this);
		}
	}
}
