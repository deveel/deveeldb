// 
//  Copyright 2010-2014 Deveel
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
using System;
using System.Linq;

using Deveel.Data.Services;

using NUnit.Framework;

namespace Deveel.Data.Diagnostics {
	[TestFixture]
	public class LoggerTests : ContextBasedTest {
		/*
		TODO:
		[Test]
		public void LogErrorToConsole() {
			var routers = SystemContext.ResolveAllServices<IEventRouter>().ToList();
			Assert.IsNotEmpty(routers);
			Assert.AreEqual(1, routers.Count);
			Assert.IsInstanceOf<LogEventRouter>(routers[0]);

			var loggers = SystemContext.ResolveAllServices<IEventLogger>().ToList();
			Assert.IsNotEmpty(loggers);
			Assert.AreEqual(1, loggers.Count);
			Assert.IsInstanceOf<ConsoleEventLogger>(loggers[0]);

			Assert.DoesNotThrow(() => Query.RegisterError("Error one"));
		}
		*/
	}
}
