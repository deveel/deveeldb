using System;
using System.Threading;

using NUnit.Framework;

namespace Deveel.Data.Diagnostics {
	[TestFixture]
	public class EventsTests : ContextBasedTest {
		[Test]
		public void AttachRouter() {
			IEvent firedEvent = null;
			Assert.DoesNotThrow(() => Query.Route<ErrorEvent>(@event => firedEvent = @event));
			Assert.IsNull(firedEvent);
		}

		[Test]
		public void RouteError() {
			var reset = new AutoResetEvent(false);

			IEvent firedEvent = null;
			Query.Route<ErrorEvent>(e => {
				firedEvent = e;
				reset.Set();
			});

			Query.OnError(new Exception("Test Error"));

			reset.WaitOne();

			Assert.IsNotNull(firedEvent);
			Assert.IsInstanceOf<ErrorEvent>(firedEvent);
		}

		[Test]
		public void FireAtLowerLevelAndListenAtHighest() {
			var reset = new AutoResetEvent(false);

			IEvent fired = null;
			System.Context.Route<InformationEvent>(e => {
				fired = e;
				reset.Set();
			});

			Query.OnVerbose("Test Message");

			reset.WaitOne(300);

			Assert.IsNotNull(fired);
			Assert.IsInstanceOf<InformationEvent>(fired);

			var infoEvent = (InformationEvent) fired;
			Assert.AreEqual(InformationLevel.Verbose, infoEvent.Level);
			Assert.AreEqual("Test Message", infoEvent.Message);
		}

		[Test]
		public void RouteOnlyOnce() {
			var reset1 = new AutoResetEvent(false);
			var reset2 = new AutoResetEvent(false);

			IEvent systemFired = null;
			System.Context.Route<InformationEvent>(e => {
				systemFired = e;
				reset1.Set();
			});

			IEvent sessionFired = null;

			Session.Context.Route<ErrorEvent>(e => {
				sessionFired = e;
				reset2.Set();
			});

			Query.OnVerbose("Test Message");

			reset1.WaitOne(300);
			reset2.WaitOne(300);

			Assert.IsNotNull(systemFired);
			Assert.IsNull(sessionFired);
		}

		[Test]
		public void RouteOnlyOnceForSameEventType() {
			var reset1 = new AutoResetEvent(false);
			var reset2 = new AutoResetEvent(false);

			IEvent systemFired = null;
			System.Context.Route<InformationEvent>(e => {
				systemFired = e;
				reset1.Set();
			});

			IEvent sessionFired = null;

			Session.Context.Route<InformationEvent>(e => {
				sessionFired = e;
				reset2.Set();
			}, e => e.Level == InformationLevel.Debug);

			Query.OnVerbose("Test Message");

			reset1.WaitOne(300);
			reset2.WaitOne(300);

			Assert.IsNotNull(systemFired);
			Assert.IsNull(sessionFired);
		}

		[Test]
		public void RouteTwiceForSameEventType() {
			var reset1 = new AutoResetEvent(false);
			var reset2 = new AutoResetEvent(false);

			IEvent systemFired = null;
			System.Context.Route<InformationEvent>(e => {
				systemFired = e;
				reset1.Set();
			});

			IEvent sessionFired = null;

			Session.Context.Route<InformationEvent>(e => {
				sessionFired = e;
				reset2.Set();
			});

			Query.OnVerbose("Test Message");

			reset1.WaitOne(300);
			reset2.WaitOne(300);

			Assert.IsNotNull(systemFired);
			Assert.IsNotNull(sessionFired);
		}

		[Test]
		public void RouteOneRegisteredMany() {
			var reset = new AutoResetEvent(false);

			QueryEvent a = null, b = null;
			System.Context.Route<QueryEvent>(e => a = e);
			System.Context.Route<QueryEvent>(e => b = e);

			IEvent fired = null;
			System.Context.Route<InformationEvent>(e => {
				fired = e;
				reset.Set();
			});

			Query.OnVerbose("Test Message");

			reset.WaitOne(300);

			Assert.IsNotNull(fired);
			Assert.IsInstanceOf<InformationEvent>(fired);

			Assert.IsNull(a);
			Assert.IsNull(b);

			var infoEvent = (InformationEvent) fired;
			Assert.AreEqual(InformationLevel.Verbose, infoEvent.Level);
			Assert.AreEqual("Test Message", infoEvent.Message);
		}
	}
}
