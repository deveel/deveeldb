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
using Xunit.Abstractions;

namespace Deveel.Data.Events {
	public class EventSourceTests {
		private ITestOutputHelper output;

		public EventSourceTests(ITestOutputHelper output) {
			this.output = output;
		}

		[Fact]
		public void CreateEnvSource() {
			IEventSource source = EventSource.Environment;

			Assert.NotNull(source.Metadata);
			Assert.NotEmpty(source.Metadata);
			Assert.Null(source.ParentSource);

			foreach (var pair in source.Metadata) {
				output.WriteLine("{0} = {1}", pair.Key, pair.Value);
			}
		}

		[Fact]
		public void CreateMockedEventSource() {
			var mock = new Mock<IEventSource>();
			mock.SetupGet(x => x.Metadata)
				.Returns(new Dictionary<string, object> {{"a", 67}});

			var source = mock.Object;

			var value = source.GetValue<int>("a");

			Assert.Equal(67, value);
		}

		[Fact]
		public void CreateEmptyEvent() {
			var id = Guid.NewGuid();
			var @event = new Event(EventSource.Environment, id);

			Assert.NotNull(@event.EventSource);
			Assert.IsAssignableFrom<EventSource>(@event.EventSource);
			Assert.Contains("Environment", @event.EventSource.GetType().Name);
			Assert.Equal(id, @event.EventId);
		}
	}
}