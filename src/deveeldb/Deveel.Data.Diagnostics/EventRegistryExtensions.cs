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

namespace Deveel.Data.Diagnostics {
	public static class EventRegistryExtensions {
		public static void Error(this IEventRegistry registry, IEventSource source, ErrorException ex) {
			registry.RegisterEvent(ex.AsEvent(source));
		}

		public static void Error(this IEventRegistry registry, IEventSource source, Exception ex) {
			var errorEvent = new ErrorEvent(source, EventClasses.Runtime, -1, ex.Message);
			errorEvent.ErrorLevel(ErrorLevel.Error);
			errorEvent.StackTrace(ex.StackTrace);
#if !PCL
			errorEvent.ErrorSource(ex.Source);
#endif

			registry.RegisterEvent(errorEvent);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, string message, string stackTrace, string errorSource) {
			Error(registry, source, errorClass, errorCode, ErrorLevel.Error, message, stackTrace, errorSource);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, ErrorLevel level) {
			Error(registry, source, errorClass, errorCode, level, null);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, ErrorLevel level, string message) {
			Error(registry, source, errorClass, errorCode, level, message, null, null);
		}

		public static void Error(this IEventRegistry registry, IEventSource source, int errorClass, int errorCode, ErrorLevel level, string message, string stackTrace, string errorSource) {
			var errorEvent = new ErrorEvent(source, errorClass, errorCode, message);
			errorEvent.ErrorLevel(level);
			errorEvent.StackTrace(stackTrace);
			errorEvent.ErrorSource(errorSource);
			registry.RegisterEvent(errorEvent);
		}
	}
}
