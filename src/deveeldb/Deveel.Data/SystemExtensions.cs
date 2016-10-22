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
using System.Linq;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Security;

namespace Deveel.Data {
	public static class SystemExtensions {
		public static bool HasFeature(this ISystem system, string name, string version) {
			return system.Features.Any(x => x.FeatureName == name && x.Version == version);
		}

		public static bool HasFeature(this ISystem system, string name) {
			return system.Features.Any(x => x.FeatureName == name);
		}

		public static IEventSource AsEventSource(this ISystem system) {
			if (system == null)
				return null;

			if (system is IEventSource)
				return (IEventSource) system;

			return new EventSource();
		}

		public static IDatabase CreateDatabase(this ISystem system, IConfiguration configuration, string adminName,
			string adminPassword) {
			return system.CreateDatabase(configuration, adminName, KnownUserIdentifications.ClearText, adminPassword);
		}
	}
}
