// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.Diagnostics {
	public static class EventSourceExtensions {
		public static void OnEvent(this IEventSource source, IEvent @event) {
			source.Context.RegisterEvent(@event);
		}

		public static void OnError(this IEventSource source, Exception error) {
			OnError(source, error, -1);
		}

		public static void OnError(this IEventSource source, Exception error, int errorCode) {
			OnError(source, error, errorCode, ErrorLevel.Error);
		}

		public static void OnError(this IEventSource source, Exception error, ErrorLevel level) {
			OnError(source, error, -1, level);
		}

		public static void OnError(this IEventSource source, Exception error, int errorCode, ErrorLevel level) {
			var errorEvent = new ErrorEvent(error, errorCode, level);
			source.OnEvent(errorEvent);
		}

		public static void OnInformation(this IEventSource source, string message) {
			source.OnInformation(message, InformationLevel.Information);
		}

		public static void OnInformation(this IEventSource source, string message, InformationLevel level) {
			source.OnEvent(new InformationEvent(message, level));
		}

		public static void OnVerbose(this IEventSource source, string message) {
			source.OnInformation(message, InformationLevel.Verbose);
		}

		public static void OnDebug(this IEventSource source, string message) {
			source.OnInformation(message, InformationLevel.Debug);
		}

		public static void OnPerformance(this IEventSource source, string key, object value) {
			source.OnEvent(new PerformanceEvent(key, value));
		}
	}
}
