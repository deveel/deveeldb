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

namespace Deveel.Data.Configurations {
	class ConfigurationBuilder : IConfigurationBuilder {
		private Configuration configuration;
		private Dictionary<string, Action<IConfigurationBuilder>> children;

		public ConfigurationBuilder() {
			configuration = new Configuration();
			children = new Dictionary<string, Action<IConfigurationBuilder>>();
		}

		public IConfigurationBuilder WithSetting(string key, object value) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException(nameof(key));

			configuration.SetValue(key, value);
			return this;
		}

		public IConfigurationBuilder WithSection(string key, Action<IConfigurationBuilder> child) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException(nameof(key));

			children[key] = child;
			return this;
		}

		public IConfiguration Build() {
			var result = configuration;
			foreach (var child in children) {
				var builder = new ConfigurationBuilder();
				child.Value(builder);

				result.AddSection(child.Key, builder.Build());
			}

			return result;
		}
	}
}
