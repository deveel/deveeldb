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

using Deveel.Data.Build;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Views {
	public sealed class ViewsFeature : ISystemFeature {
		public string Name {
			get { return "Views"; }
		}

		public string Version {
			get { return "2.0"; }
		}

		public void OnBuild(ISystemBuilder builder) {
			builder.Use<IObjectManager>(options => options
					.With<ViewManager>()
					.InTransactionScope()
					.HavingKey(DbObjectType.View))
				.Use<ITableCompositeCreateCallback>(options => options
					.With<ViewsInit>()
					.HavingKey("Views")
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<ViewTableContainer>()
					.InTransactionScope());
		}

		public void OnSystemEvent(SystemEvent @event) {
			throw new System.NotImplementedException();
		}
	}
}