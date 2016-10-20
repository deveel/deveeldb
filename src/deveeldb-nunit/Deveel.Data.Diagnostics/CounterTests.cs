using System;
using System.Threading;

using NUnit.Framework;

namespace Deveel.Data.Diagnostics {
	[TestFixture]
	public class CounterTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			Database.Counters.Add(new SystemPerformanceCounter(KnownSystemCounterNames.MachineMemoryUsage));
			Database.Counters.Add(new SystemPerformanceCounter(KnownSystemCounterNames.MachineProcessorUsage));
			Database.Counters.Add(new SystemPerformanceCounter(KnownSystemCounterNames.SystemMemoryUsage));
			Database.Counters.Add(new SystemPerformanceCounter(KnownSystemCounterNames.SystemProcessorUsage));

			Thread.Sleep(400);
		}

		[TestCase(KnownSystemCounterNames.MachineMemoryUsage)]
		[TestCase(KnownSystemCounterNames.MachineProcessorUsage)]
		[TestCase(KnownSystemCounterNames.SystemMemoryUsage)]
		[TestCase(KnownSystemCounterNames.SystemProcessorUsage)]
		public void CountPerformance(string name) {
			ICounter counter;
			Assert.IsTrue(Database.Counters.TryCount(name, out counter));
			Assert.IsNotNull(counter);
			Assert.IsNotNull(counter.Value);
		}
	}
}
