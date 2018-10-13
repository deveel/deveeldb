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

namespace Deveel.Data.Configurations {
	public static class ContextExtensions {
		public static T GetValue<T>(this IContext context, string key, T defaultValue) {
			var current = context;
			while (current != null) {
				if (current is IConfigurationScope) {
					var scope = (IConfigurationScope) current;
					var config = scope.Configuration;
					return config.GetValue(key, defaultValue);
				}

				current = current.ParentContext;
			}

			return defaultValue;
		}

		public static T GetValue<T>(this IContext context, string key)
			=> context.GetValue<T>(key, default(T));
	}
}