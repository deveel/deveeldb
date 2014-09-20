// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Data.Common;
using System.Reflection;

namespace Deveel.Data.Client {
	public sealed class DeveelDbProviderFactory : DbProviderFactory, IServiceProvider {
		private static Type dbProviderServices;

		public override bool CanCreateDataSourceEnumerator {
			get { return false; }
		}

		private static Type GetDbProviderServices() {
			if (dbProviderServices == null) {
				dbProviderServices =
					Type.GetType(
						"System.Data.Common.DbProviderServices, System.Data.Entity, " +
						"Version=3.5.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089", false, true);
			}

			return dbProviderServices;
		}

		private static object CreateDeveelDbServicesProvider() {
			//TODO: we should find a better way to handle also the version and the public token...
			const string typeName = "Deveel.Data.Entity.DeveelDbProviderServices, DeveelDb.Entity";
			Type type = Type.GetType(typeName, false);
			if (type == null)
				return null;

			return Activator.CreateInstance(type, true);
		}

		public object GetService(Type serviceType) {
			Type dbServicesType = GetDbProviderServices();
			if (serviceType != dbServicesType)
				return null;

			object service = CreateDeveelDbServicesProvider();
			return service;
		}

		public override DbCommand CreateCommand() {
			return new DeveelDbCommand();
		}

		public override DbConnection CreateConnection() {
			return new DeveelDbConnection();
		}

		public override DbDataAdapter CreateDataAdapter() {
			return new DeveelDbDataAdapter();
		}

		public override DbParameter CreateParameter() {
			return new DeveelDbParameter();
		}
	}
}