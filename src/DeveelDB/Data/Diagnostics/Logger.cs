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
using System.Threading.Tasks;

namespace Deveel.Data.Diagnostics {
	public abstract class Logger : ILogger {
		static Logger() {
			Empty = new EmptyLogger();
		}

		public static ILogger Empty { get; }

		public abstract bool IsInterestedIn(LogLevel level);

		public abstract Task LogAsync(LogEntry entry);

		#region EmptyLogger

		class EmptyLogger : Logger {
			public override bool IsInterestedIn(LogLevel level) {
				return true;
			}

			public override Task LogAsync(LogEntry entry) {
				return Task.CompletedTask;
			}
		}

		#endregion
	}
}