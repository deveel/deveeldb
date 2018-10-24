using System;
using System.Collections.Generic;


using Moq;

using Xunit;

namespace Deveel.Data.Events {
	public class EventRegistryTests {
		private IEventRegistry registry;
		private List<IEvent> events;

		public EventRegistryTests() {
			events = new List<IEvent>();

			var mock = new Mock<IEventRegistry>();
			mock.Setup(x => x.Register(It.IsAny<IEvent>()))
				.Callback<IEvent>(e => events.Add(e));
			mock.SetupGet(x => x.EventType)
				.Returns(typeof(Event));

			registry = mock.Object;
		}

		[Fact]
		public void RegisterCreatedEvent() {
			registry.Register(new Event(EventSource.Environment, -1));

			Assert.NotEmpty(events);
			Assert.Single(events);
		}

		[Fact]
		public void RegisterEventToBeBuilt() {
			registry.Register<Event>(EventSource.Environment, -1);

			Assert.NotEmpty(events);
			Assert.Single(events);
		}
	}
}