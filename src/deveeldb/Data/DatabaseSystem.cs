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

using Deveel.Data.Configurations;
using Deveel.Data;
using Deveel.Data.Events;
using Deveel.Data.Services;

namespace Deveel.Data {
	public sealed class DatabaseSystem : IDatabaseSystem, IEventHandler {
		private Dictionary<string, IDatabase> databases;
		private bool started;
		private ServiceContainer container;
		private bool ownsContainer;
		private IScope scope;
		private InMemoryEventRegistry eventRegistry;

		private DatabaseSystem(ServiceContainer container, bool ownsContainer, IConfiguration configuration) {
			this.container = container;

			Configuration = configuration;
			container.RegisterInstance<IDatabaseSystem>(this);

			this.ownsContainer = ownsContainer;

			scope = container.OpenScope(KnownScopes.System);

			eventRegistry = new InMemoryEventRegistry();
		}

		public DatabaseSystem(ServiceContainer container, IConfiguration configuration)
			: this(container, false, configuration) {
		}

		public DatabaseSystem(IConfiguration configuration)
			: this(new ServiceContainer(), false, configuration) {
		}

		IEventSource IEventSource.ParentSource => null;

		IDictionary<string, object> IEventSource.Metadata => new Dictionary<string, object>();

		public IConfiguration Configuration { get; }

		IScope IContext.Scope => scope;

		IContext IContext.ParentContext => null;

		string IContext.ContextName => KnownScopes.System;

		IEventRegistry IEventHandler.Registry => DiscoverRegistry();

		private IEventRegistry DiscoverRegistry() {
			// TODO:
			return eventRegistry;
		}

		private void AssertStarted() {
			if (!started)
				throw new SystemException("The system was not started");
		}

		private void DiscoveryDatabases() {
			// TODO:
		}

		public void Start() {
			databases = new Dictionary<string, IDatabase>();

			DiscoveryDatabases();

			started = true;
		}

		public IEnumerable<string> GetDatabases() {
			AssertStarted();

			return databases.Keys;
		}

		IDatabase IDatabaseSystem.CreateDatabase(string name, IConfiguration configuration,
			IEnumerable<IDatabaseFeature> features)
			=> CreateDatabase(name, configuration, features);

		public Database CreateDatabase(string name, IConfiguration configuration, IEnumerable<IDatabaseFeature> features) {
			AssertStarted();

			if (databases.ContainsKey(name))
				throw new ArgumentException($"Database '{name}' already exists in this system");

			Database database;

			try {
				database = new Database(this, name, configuration);
				database.Create(features);

				databases[name] = database;
			} catch (Exception ex) {
				throw new SystemException("An error occurred while creating the database", ex);
			}

			return database;
		}

		public bool DatabaseExists(string databaseName) {
			AssertStarted();

			if (!databases.TryGetValue(databaseName, out var database))
				return false;

			
			return databases.ContainsKey(databaseName);
		}

		public IDatabase OpenDatabase(string databaseName) {
			AssertStarted();

			throw new NotImplementedException();
		}

		public bool DeleteDatabase(string databaseName) {
			AssertStarted();

			throw new NotImplementedException();
		}

		public void Dispose() {
			foreach (var database in databases.Values) {
				database?.Dispose();
			}

			databases.Clear();

			scope.Dispose();

			if (ownsContainer)
				container?.Dispose();

			eventRegistry?.Dispose();
			started = false;
		}
	}
}