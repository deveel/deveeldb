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
using System.Collections.Generic;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Sql.Views;
using Deveel.Data.Store;

namespace Deveel.Data {
	public class SystemBuilder : ISystemBuilder {
		public SystemBuilder() {
			ServiceContainer = new ServiceContainer();
		}

		public ServiceContainer ServiceContainer { get; private set; }

		public static SystemBuilder Default {
			get {
				var builder = new SystemBuilder();
				RegisterDefaultServices(builder);
				return builder;
			}
		}

		ISystemBuilder ISystemBuilder.Use(ServiceUseOptions options) {
			return Use(options);
		}

		public SystemBuilder Use(ServiceUseOptions options) {
			if (options == null)
				throw new ArgumentNullException("options");

			options.Validate();

			var key = options.Key;
			var type = options.ServiceType;
			var policy = options.Policy;

			var registration = new ServiceRegistration(type, options.ImplementationType) {
				Instance = options.Instance,
				Scope = options.Scope,
				ServiceKey = key
			};

			if (key != null) {
				if (policy == ServiceUsePolicy.Replace) {
					if (ServiceContainer.IsRegistered(type, key))
						ServiceContainer.Unregister(type, key);
				}

				ServiceContainer.Register(registration);
			} else {
				if (policy == ServiceUsePolicy.Replace) {
					if (ServiceContainer.IsRegistered(type))
						ServiceContainer.Unregister(type);

					ServiceContainer.Register(registration);
				}
			}

			return this;
		}

		private static void RegisterDefaultServices(ISystemBuilder builder) {
#if !MICRO
			builder.UseSecurity();

			builder.UseDefaultSqlCompiler()
				.UseDefaultStatementCache();
#endif

			builder.UseDefaultQueryPlanner();

			builder.UseLocalFileSystem()
				.UseInMemoryStoreSystem()
				.UseSingleFileStoreSystem()
				.UseJournaledStoreSystem()
				.UseScatteringFileDataFactory();
		}

		private ISystemContext BuildContext(out IEnumerable<FeatureInfo> modules) {
			modules = LoadFeatures();

			return new SystemContext(ServiceContainer);
		}

		private IEnumerable<FeatureInfo> LoadFeatures() {
			var featureInfos = new List<FeatureInfo>();

			var features = ServiceContainer.ResolveAll<ISystemFeature>();
			foreach (var feature in features) {
				feature.OnBuild(this);

				featureInfos.Add(new FeatureInfo(feature.Name, feature.Version));
			}

			ServiceContainer.Unregister<ISystemFeature>();
			return featureInfos;
		}

		public ISystem Build() {
			// ensure the required components are configured in the builder
			// TODO: in a future revision they will be optional too
			this.UseTables()
				.UseRoutines()
				.UseSchema()
				.UseViews()
				.UseSequences()
				.UseTriggers()
				.UseTypes()
				.UseVariables();

			IEnumerable<FeatureInfo> modules;
			var context = BuildContext(out modules);
			return new DatabaseSystem(context, modules);
		}
	}
}
