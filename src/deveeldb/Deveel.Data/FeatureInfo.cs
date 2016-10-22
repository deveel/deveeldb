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
using System.Reflection;

using Deveel.Data.Build;

namespace Deveel.Data {
	public sealed class FeatureInfo {
		private Action<SystemEvent> systemEventHandler;

		private FeatureInfo(string featureName, string version) {
			if (String.IsNullOrEmpty(featureName))
				throw new ArgumentNullException("featureName");

			FeatureName = featureName;
			Version = version;
		}

		public string FeatureName { get; private set; }

		public string Version { get; private set; }

		internal static FeatureInfo BuildFrom(ISystemFeature feature) {
			var info = new FeatureInfo(feature.Name, feature.Version);
			info.systemEventHandler = feature.OnSystemEvent;
			return info;
		}

		internal void OnSystemEvent(SystemEventType eventType, IQuery systemQuery) {
			systemEventHandler(new SystemEvent(eventType, systemQuery));
		}
	}
}
