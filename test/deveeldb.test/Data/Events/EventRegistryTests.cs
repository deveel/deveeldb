// 
//  Copyright 2010-2018 Deveel
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

			registry = mock.Object;
		}

		[Fact]
		public void RegisterCreatedEvent() {
			registry.Register(new Event(EventSource.Environment));

			Assert.NotEmpty(events);
			Assert.Single(events);
		}

		[Fact]
		public void RegisterEventToBeBuilt() {
			registry.Register<Event>(EventSource.Environment);

			Assert.NotEmpty(events);
			Assert.Single(events);
		}
	}
}