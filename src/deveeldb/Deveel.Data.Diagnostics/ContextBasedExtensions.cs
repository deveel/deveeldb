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

namespace Deveel.Data.Diagnostics {
	public static class ContextBasedExtensions {
		public static void OnEvent(this IContextBased obj, IEvent @event) {
			if (@event.EventSource == null &&
			    obj is IEventSource)
				@event.EventSource = (IEventSource) obj;

			obj.Context.RegisterEvent(@event);
		}

		public static void OnError(this IContextBased obj, Exception error) {
			obj.OnError(error, -1);
		}

		public static void OnError(this IContextBased obj, Exception error, int errorCode) {
			obj.OnError(error, errorCode, ErrorLevel.Error);
		}

		public static void OnError(this IContextBased obj, Exception error, ErrorLevel level) {
			obj.OnError(error, -1, level);
		}

		public static void OnError(this IContextBased obj, Exception error, int errorCode, ErrorLevel level) {
			var errorEvent = new ErrorEvent(error, errorCode, level);
			obj.OnEvent(errorEvent);
		}

		public static void OnWarning(this IContextBased obj, Exception warning) {
			obj.OnWarning(warning, -1);
		}

		public static void OnWarning(this IContextBased obj, Exception warning, int errorCode) {
			obj.OnError(warning, errorCode, ErrorLevel.Warning);
		}

		public static void OnInformation(this IContextBased obj, string message) {
			obj.OnInformation(message, InformationLevel.Information);
		}

		public static void OnInformation(this IContextBased obj, string message, InformationLevel level) {
			obj.OnEvent(new InformationEvent(message, level));
		}

		public static void OnVerbose(this IContextBased obj, string message) {
			obj.OnInformation(message, InformationLevel.Verbose);
		}

		public static void OnDebug(this IContextBased obj, string message) {
			obj.OnInformation(message, InformationLevel.Debug);
		}

		public static void OnCounter(this IContextBased obj, string key) {
			obj.OnCounter(key, null);
		}

		public static void OnCounter(this IContextBased obj, string key, object value) {
			obj.OnEvent(new CounterEvent(key, value));
		}

		public static void UseLogger<TLogger>(this IContextBased obj, TLogger logger) where TLogger : LoggerBase {
			obj.Context.AttachRouter(logger);
		}

	}
}
